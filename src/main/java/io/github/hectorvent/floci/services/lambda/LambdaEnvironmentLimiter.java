package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayDeque;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Counts the physical Lambda execution environments Floci runs, across every function, account
 * and region, and caps them when {@code floci.services.lambda.max-physical-environments} is set.
 *
 * <p>Every environment holds one {@link Slot} from the moment its launch is admitted until its
 * teardown is confirmed: on-demand warm-pool containers, provisioned-concurrency environments, and
 * containers an earlier Floci process left behind. A slot is {@code active} while it launches or
 * serves an invocation, {@code idle} while it waits warm, and {@code retiring} while it is being
 * stopped; a stop that fails keeps the slot retiring, so the cap never reads free while the
 * container may still run.
 *
 * <p>Logical concurrency ({@link LambdaConcurrencyLimiter}) is separate: this bounds the
 * containers the host runs, not the invocations AWS would accept. With the cap set, an admission
 * that finds no warm environment to reuse and no free slot queues behind earlier ones in arrival
 * order. The head of the queue asks the warm pool to retire its least-recently-used idle
 * environment, since a warm container is only a cache, and otherwise waits for a slot until
 * {@code physical-environment-wait-timeout-seconds} runs out. Unset, nothing waits and the
 * slots are only counted.
 */
@ApplicationScoped
public class LambdaEnvironmentLimiter {

    /** {@link Status#available()} when there is no cap, the largest value the contract allows. */
    public static final int UNLIMITED = Integer.MAX_VALUE;
    private static final int DEFAULT_WAIT_SECONDS = 15;

    private final int limit;
    private final int waitSeconds;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition changed = lock.newCondition();
    private final ArrayDeque<Object> queue = new ArrayDeque<>();
    private int active;
    private int idle;
    private int retiring;
    private long changes;
    private volatile BooleanSupplier idleRetirement = () -> false;

    @Inject
    public LambdaEnvironmentLimiter(EmulatorConfig config) {
        this(configuredLimit(config.services().lambda().maxPhysicalEnvironments()),
                config.services().lambda().physicalEnvironmentWaitTimeoutSeconds());
    }

    /** Package-private: a limiter with {@code limit} slots, or no cap when it is not positive. */
    LambdaEnvironmentLimiter(int limit, int waitSeconds) {
        this.limit = Math.max(0, limit);
        this.waitSeconds = Math.max(0, waitSeconds);
    }

    /** An uncapped limiter, which only counts. */
    public LambdaEnvironmentLimiter() {
        this(0, DEFAULT_WAIT_SECONDS);
    }

    private static int configuredLimit(OptionalInt configured) {
        if (configured == null || configured.isEmpty()) {
            return 0;
        }
        if (configured.getAsInt() < 1) {
            throw new IllegalArgumentException("floci.services.lambda.max-physical-environments must be at "
                    + "least 1, or unset for no cap; received " + configured.getAsInt());
        }
        return configured.getAsInt();
    }

    public boolean bounded() {
        return limit > 0;
    }

    /**
     * Sets how a queued admission makes room: stop one idle environment and say whether it did.
     * The warm pool registers its least-recently-used eviction here.
     */
    void retireIdleWith(BooleanSupplier retirement) {
        this.idleRetirement = retirement;
    }

    /**
     * Admits one environment: a warm one {@code reuse} hands back, which keeps the slot it already
     * holds, or a new {@link Slot} to launch with. {@code reuse} runs without this limiter's lock
     * and may block, for instance on a liveness probe; it is asked again whenever capacity or the
     * pool changes, so a warm environment released while this admission waits is taken up.
     *
     * @throws AdmissionTimeoutException when the cap is set and nothing was admitted in time
     * @throws AdmissionInterruptedException when the waiting thread is interrupted; its interrupt
     *         flag is restored
     */
    public <T> Admission<T> admit(Supplier<T> reuse) {
        if (!bounded()) {
            T warm = reuse.get();
            return warm != null ? new Admission<>(warm, null) : new Admission<>(null, newSlot());
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(waitSeconds);
        Object ticket = new Object();
        lock.lock();
        try {
            queue.addLast(ticket);
        } finally {
            lock.unlock();
        }
        try {
            while (true) {
                T warm = reuse.get();
                if (warm != null) {
                    return new Admission<>(warm, null);
                }
                boolean head;
                long seen;
                lock.lock();
                try {
                    head = queue.peekFirst() == ticket;
                    if (head && total() < limit) {
                        queue.removeFirst();
                        changes++;
                        changed.signalAll();
                        return new Admission<>(null, slot(SlotState.ACTIVE));
                    }
                    seen = changes;
                } finally {
                    lock.unlock();
                }
                if (head && idleRetirement.getAsBoolean()) {
                    continue;
                }
                awaitChange(seen, deadline);
            }
        } finally {
            lock.lock();
            try {
                if (queue.remove(ticket)) {
                    changes++;
                    changed.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    private void awaitChange(long seen, long deadline) {
        lock.lock();
        try {
            while (changes == seen) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new AdmissionTimeoutException(limit, waitSeconds);
                }
                changed.awaitNanos(remaining);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdmissionInterruptedException(e);
        } finally {
            lock.unlock();
        }
    }

    /** A slot for an environment launched outside admission, which never waits. */
    Slot newSlot() {
        lock.lock();
        try {
            return slot(SlotState.ACTIVE);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Counts a container an earlier Floci process left running as retiring until its removal is
     * confirmed. It is never reused and never waits, so the count can briefly exceed the cap.
     */
    public Slot reserveRecovered() {
        lock.lock();
        try {
            return slot(SlotState.RETIRING);
        } finally {
            lock.unlock();
        }
    }

    /** Wakes queued admissions to look again, as when the warm pool stops admitting. */
    void signal() {
        lock.lock();
        try {
            changes++;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    void markActive(Slot slot) {
        move(slot, SlotState.ACTIVE);
    }

    void markIdle(Slot slot) {
        move(slot, SlotState.IDLE);
    }

    void markRetiring(Slot slot) {
        move(slot, SlotState.RETIRING);
    }

    public Status status() {
        lock.lock();
        try {
            int total = total();
            int available = bounded() ? Math.max(0, limit - total) : UNLIMITED;
            return new Status(limit, total, active, idle, retiring, queue.size(), available);
        } finally {
            lock.unlock();
        }
    }

    private int total() {
        return active + idle + retiring;
    }

    private Slot slot(SlotState state) {
        Slot slot = new Slot(this, state);
        count(state, 1);
        return slot;
    }

    private void move(Slot slot, SlotState target) {
        if (slot == null) {
            return;
        }
        lock.lock();
        try {
            if (slot.state == SlotState.CLOSED || slot.state == target) {
                return;
            }
            count(slot.state, -1);
            count(target, 1);
            slot.state = target;
            changes++;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void release(Slot slot) {
        lock.lock();
        try {
            if (slot.state == SlotState.CLOSED) {
                return;
            }
            count(slot.state, -1);
            slot.state = SlotState.CLOSED;
            changes++;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void count(SlotState state, int delta) {
        switch (state) {
            case ACTIVE -> active += delta;
            case IDLE -> idle += delta;
            case RETIRING -> retiring += delta;
            case CLOSED -> {
                // A closed slot is no longer counted.
            }
        }
    }

    private enum SlotState { ACTIVE, IDLE, RETIRING, CLOSED }

    /** One physical environment's place under the cap; closing it, once, frees the place. */
    public static final class Slot implements AutoCloseable {
        private final LambdaEnvironmentLimiter owner;
        private SlotState state;

        private Slot(LambdaEnvironmentLimiter owner, SlotState state) {
            this.owner = owner;
            this.state = state;
        }

        @Override
        public void close() {
            owner.release(this);
        }
    }

    /** Either a reused warm environment, which keeps its slot, or a new slot to launch with. */
    public record Admission<T>(T reused, Slot slot) {
    }

    /**
     * Physical environments now: {@code total} is {@code active + idle + retiring}, and
     * {@code available} is {@code configuredLimit - total}, or {@link #UNLIMITED} when
     * {@code configuredLimit} is 0 (no cap). {@code queued} admissions hold no slot yet.
     */
    public record Status(int configuredLimit, int total, int active, int idle, int retiring, int queued,
                         int available) {
    }

    /** No physical environment became available within the configured wait. */
    public static class AdmissionTimeoutException extends RuntimeException {
        AdmissionTimeoutException(int limit, int waitSeconds) {
            super("Timed out after " + waitSeconds + "s waiting for one of " + limit
                    + " physical Lambda execution environments");
        }
    }

    /** The thread waiting for a physical environment was interrupted. */
    public static class AdmissionInterruptedException extends RuntimeException {
        AdmissionInterruptedException(InterruptedException cause) {
            super("Interrupted waiting for a physical Lambda execution environment", cause);
        }
    }
}
