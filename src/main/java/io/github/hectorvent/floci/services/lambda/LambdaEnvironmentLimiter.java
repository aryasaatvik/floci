package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.config.EmulatorConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Admits physical Lambda execution environments independently from Lambda's logical
 * concurrency limiter.
 *
 * <p>The limiter is disabled when {@code max-physical-environments} is absent. In that
 * mode every acquisition still has a close-once permit and contributes to status, but
 * no caller waits and the existing AWS-like emulator behavior is unchanged.
 *
 * <p>When enabled, waiters are admitted in FIFO order. A waiter has one bounded,
 * interruptible wait; timed-out or interrupted waiters are removed before returning so
 * a later release cannot accidentally grant a permit to a cancelled invocation. A reusable
 * warm environment can satisfy a waiter without allocating a new permit, even when the
 * physical cap is full.
 */
@ApplicationScoped
public class LambdaEnvironmentLimiter implements AutoCloseable {

    /** A non-positive configured value means that the physical cap is disabled. */
    private static final int UNBOUNDED = 0;
    private static final int DEFAULT_WAIT_SECONDS = 15;

    private final int maxPhysicalEnvironments;
    private final long waitNanos;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition changed = lock.newCondition();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final Map<String, Integer> totalByKey = new HashMap<>();
    private final Map<String, Integer> activeByKey = new HashMap<>();
    private final Map<String, Integer> idleByKey = new HashMap<>();
    private final Map<String, Integer> queuedByKey = new HashMap<>();
    private long granted;
    private long timedOut;
    private long interrupted;
    private int total;
    private int active;
    private int idle;
    private boolean closed;

    private static final class Waiter {
        private final String key;
        private final Supplier<?> reusable;

        private Waiter(String key, Supplier<?> reusable) {
            this.key = key;
            this.reusable = reusable;
        }
    }

    @Inject
    public LambdaEnvironmentLimiter(EmulatorConfig config) {
        this(configuredLimit(config), configuredWaitSeconds(config));
    }

    /** Test-only constructor. A non-positive limit disables the physical cap. */
    LambdaEnvironmentLimiter(int maxPhysicalEnvironments, int waitSeconds) {
        this.maxPhysicalEnvironments = Math.max(UNBOUNDED, maxPhysicalEnvironments);
        this.waitNanos = TimeUnit.SECONDS.toNanos(Math.max(0, waitSeconds));
    }

    /** Test-only constructor for the unchanged, unbounded default behavior. */
    LambdaEnvironmentLimiter() {
        this(UNBOUNDED, DEFAULT_WAIT_SECONDS);
    }

    /**
     * Acquires a physical execution-environment slot for {@code environmentKey}.
     *
     * <p>The public method preserves the existing no-checked-exception shape used by
     * Lambda services. If an invocation thread is interrupted while waiting, the
     * interrupt flag is restored and an {@link AdmissionInterruptedException} is
     * thrown so the caller can return a bounded invocation failure.
     */
    public Permit acquire(String environmentKey) {
        return acquire(environmentKey, () -> null).permit();
    }

    /**
     * Acquires capacity for a new environment, or atomically reserves a reusable warm
     * environment supplied by {@code reusable}. The supplier is invoked while admission
     * is serialized, so a waiter cannot observe a warm handle and then lose it to another
     * waiter before returning to its caller.
     *
     * <p>The returned admission has exactly one non-null member: {@link Admission#reusable()}
     * when the supplier removed a warm environment, or {@link Admission#permit()} when a new
     * physical environment may be created.
     */
    public <T> Admission<T> acquire(String environmentKey, Supplier<T> reusable) {
        // Keep the opt-in nature of this limiter observable: without a physical cap there is
        // no wait to interrupt, so retain the historical non-blocking acquisition semantics
        // even if a caller arrives with a stale interrupt flag.
        if (!bounded()) {
            String key = normalizeKey(environmentKey);
            lock.lock();
            try {
                ensureOpen(key);
                T warm = reusable.get();
                return warm != null ? Admission.reused(warm) : Admission.permitted(grant(key));
            } finally {
                lock.unlock();
            }
        }
        try {
            return acquireInterruptibly(environmentKey, reusable);
        } catch (java.lang.InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdmissionInterruptedException(normalizeKey(environmentKey), e);
        }
    }

    /**
     * Interruptible form used by tests and by callers that already expose checked
     * interruption.
     */
    public Permit acquireInterruptibly(String environmentKey) throws java.lang.InterruptedException {
        return acquireInterruptibly(environmentKey, () -> null).permit();
    }

    /** Interruptible generic form used by warm-pool admission. */
    public <T> Admission<T> acquireInterruptibly(String environmentKey,
                                                  Supplier<T> reusable)
            throws java.lang.InterruptedException {
        String key = normalizeKey(environmentKey);
        lock.lockInterruptibly();
        Waiter waiter = null;
        try {
            ensureOpen(key);
            if (waiters.isEmpty()) {
                T warm = reusable.get();
                if (warm != null) {
                    return Admission.reused(warm);
                }
                if (!bounded() || total < maxPhysicalEnvironments) {
                    return Admission.permitted(grant(key));
                }
            }

            waiter = new Waiter(key, reusable);
            waiters.addLast(waiter);
            increment(queuedByKey, key);
            long startedAt = System.nanoTime();
            while (true) {
                ensureOpenOrRemove(waiter, key);
                if (isFirstForKey(waiter)) {
                    @SuppressWarnings("unchecked")
                    T warm = (T) waiter.reusable.get();
                    if (warm != null) {
                        waiters.remove(waiter);
                        decrement(queuedByKey, key);
                        return Admission.reused(warm);
                    }
                }
                if (waiter == waiters.peekFirst() && (!bounded() || total < maxPhysicalEnvironments)) {
                    waiters.removeFirst();
                    decrement(queuedByKey, key);
                    return Admission.permitted(grant(key));
                }

                long remaining = waitNanos - (System.nanoTime() - startedAt);
                if (remaining <= 0) {
                    removeWaiter(waiter);
                    timedOut++;
                    changed.signalAll();
                    throw new AdmissionTimeoutException(key, maxPhysicalEnvironments, waitNanos);
                }
                try {
                    changed.awaitNanos(remaining);
                } catch (java.lang.InterruptedException e) {
                    removeWaiter(waiter);
                    interrupted++;
                    changed.signalAll();
                    throw e;
                }
            }
        } catch (AdmissionClosedException e) {
            // close() wakes every waiter. Remove this waiter before propagating so status
            // cannot retain a cancelled queue entry.
            if (waiter != null) {
                removeWaiter(waiter);
            }
            changed.signalAll();
            throw e;
        } catch (RuntimeException | Error e) {
            // A reusable-resource callback is owned by the warm pool. If it fails while a
            // waiter is queued, remove that waiter before propagating so the queue cannot retain
            // a dead acquisition attempt indefinitely.
            if (waiter != null) {
                removeWaiter(waiter);
            }
            changed.signalAll();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpenOrRemove(Waiter waiter, String key) {
        if (closed) {
            removeWaiter(waiter);
            changed.signalAll();
            throw new AdmissionClosedException(key);
        }
    }

    /**
     * A warm environment can only be reserved by the first queued waiter for its identity.
     * This preserves FIFO ordering for concurrent invocations of one environment while
     * allowing unrelated function/account/version identities to reuse their own idle handles.
     */
    private boolean isFirstForKey(Waiter target) {
        for (Waiter waiter : waiters) {
            if (waiter == target) {
                return true;
            }
            if (waiter.key.equals(target.key)) {
                return false;
            }
        }
        return false;
    }

    /** Wakes queued acquisition so it can claim a warm environment released by the pool. */
    public void signalReusable(String environmentKey) {
        lock.lock();
        try {
            if (!waiters.isEmpty()) {
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private Permit grant(String key) {
        total++;
        active++;
        increment(totalByKey, key);
        increment(activeByKey, key);
        granted++;
        return new PermitImpl(this, key);
    }

    private void release(PermitImpl permit) {
        lock.lock();
        try {
            // A close-once Permit should make this branch impossible. Keep it
            // defensive so a malformed caller cannot make status negative.
            if (total <= 0) {
                return;
            }
            if (permit.state == PermitState.CLOSED) {
                return;
            }
            if (permit.state == PermitState.ACTIVE) {
                active--;
                decrement(activeByKey, permit.key);
            } else {
                idle--;
                decrement(idleByKey, permit.key);
            }
            total--;
            decrement(totalByKey, permit.key);
            permit.state = PermitState.CLOSED;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Marks a live environment as retained by the warm pool rather than actively serving. */
    void markIdle(Permit permit) {
        transition(permit, PermitState.IDLE);
    }

    /**
     * Marks a live environment as idle only if it still belongs to the acquisition that returned
     * it. Warm-pool publication and this state transition intentionally use separate locks to
     * avoid a pool/limiter lock inversion; the generation check closes the hand-off race where a
     * second invocation can reclaim the handle before the first release marks its permit idle.
     */
    boolean markIdle(Permit permit, long expectedGeneration) {
        if (!(permit instanceof PermitImpl permitImpl) || permitImpl.owner != this) {
            throw new IllegalArgumentException("Permit belongs to a different Lambda environment limiter");
        }
        lock.lock();
        try {
            if (permitImpl.state == PermitState.CLOSED
                    || permitImpl.generation != expectedGeneration) {
                return false;
            }
            transitionUnsafe(permitImpl, PermitState.IDLE);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Marks a retained warm environment as actively serving an invocation. */
    long markActive(Permit permit) {
        if (!(permit instanceof PermitImpl permitImpl) || permitImpl.owner != this) {
            throw new IllegalArgumentException("Permit belongs to a different Lambda environment limiter");
        }
        lock.lock();
        try {
            if (permitImpl.state == PermitState.CLOSED) {
                throw new IllegalStateException("Cannot activate a closed Lambda environment permit");
            }
            transitionUnsafe(permitImpl, PermitState.ACTIVE);
            permitImpl.generation++;
            return permitImpl.generation;
        } finally {
            lock.unlock();
        }
    }

    private void transition(Permit permit, PermitState target) {
        if (!(permit instanceof PermitImpl permitImpl) || permitImpl.owner != this) {
            throw new IllegalArgumentException("Permit belongs to a different Lambda environment limiter");
        }
        lock.lock();
        try {
            transitionUnsafe(permitImpl, target);
        } finally {
            lock.unlock();
        }
    }

    private void transitionUnsafe(PermitImpl permitImpl, PermitState target) {
        if (permitImpl.state == PermitState.CLOSED || permitImpl.state == target) {
            return;
        }
        if (permitImpl.state == PermitState.ACTIVE) {
            active--;
            decrement(activeByKey, permitImpl.key);
        } else {
            idle--;
            decrement(idleByKey, permitImpl.key);
        }
        if (target == PermitState.ACTIVE) {
            active++;
            increment(activeByKey, permitImpl.key);
        } else if (target == PermitState.IDLE) {
            idle++;
            increment(idleByKey, permitImpl.key);
        }
        permitImpl.state = target;
    }

    private void ensureOpen(String key) {
        if (closed) {
            throw new AdmissionClosedException(key);
        }
    }

    private void removeWaiter(Waiter waiter) {
        if (waiters.remove(waiter)) {
            decrement(queuedByKey, waiter.key);
        }
    }

    /**
     * Returns a point-in-time readback of physical admission. The returned object is
     * immutable and safe to log or expose to a status endpoint.
     */
    public Status status() {
        lock.lock();
        try {
            return new Status(maxPhysicalEnvironments, total, active, idle, waiters.size(),
                    availableUnsafe(), granted, timedOut, interrupted, closed);
        } finally {
            lock.unlock();
        }
    }

    /** Returns status scoped to one account/function/version identity. */
    public KeyStatus status(String environmentKey) {
        String key = normalizeKey(environmentKey);
        lock.lock();
        try {
            return new KeyStatus(key, totalByKey.getOrDefault(key, 0), activeByKey.getOrDefault(key, 0),
                    idleByKey.getOrDefault(key, 0), queuedByKey.getOrDefault(key, 0));
        } finally {
            lock.unlock();
        }
    }

    public int configuredLimit() {
        return maxPhysicalEnvironments;
    }

    /** Number of live physical environments (active plus retained warm). */
    public int totalCount() {
        lock.lock();
        try {
            return total;
        } finally {
            lock.unlock();
        }
    }

    /** Number of invocations waiting for a new environment or matching warm reuse. */
    public int queuedCount() {
        lock.lock();
        try {
            return waiters.size();
        } finally {
            lock.unlock();
        }
    }

    public int activeCount() {
        lock.lock();
        try {
            return active;
        } finally {
            lock.unlock();
        }
    }

    public int idleCount() {
        lock.lock();
        try {
            return idle;
        } finally {
            lock.unlock();
        }
    }

    public boolean bounded() {
        return maxPhysicalEnvironments > UNBOUNDED;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private int availableUnsafe() {
        return bounded() ? Math.max(0, maxPhysicalEnvironments - total) : Integer.MAX_VALUE;
    }

    private static String normalizeKey(String key) {
        return key == null || key.isBlank() ? "unknown" : key;
    }

    private static void increment(Map<String, Integer> counts, String key) {
        counts.merge(key, 1, Integer::sum);
    }

    private static void decrement(Map<String, Integer> counts, String key) {
        counts.computeIfPresent(key, (ignored, count) -> count <= 1 ? null : count - 1);
    }

    private static int configuredLimit(EmulatorConfig config) {
        if (config == null || config.services() == null || config.services().lambda() == null) {
            return UNBOUNDED;
        }
        OptionalInt configured = config.services().lambda().maxPhysicalEnvironments();
        return configured == null || configured.isEmpty() ? UNBOUNDED : configured.getAsInt();
    }

    private static int configuredWaitSeconds(EmulatorConfig config) {
        if (config == null || config.services() == null || config.services().lambda() == null) {
            return DEFAULT_WAIT_SECONDS;
        }
        return Math.max(0, config.services().lambda().physicalEnvironmentWaitTimeoutSeconds());
    }

    @FunctionalInterface
    public interface Permit extends AutoCloseable {
        @Override
        void close();
    }

    private enum PermitState { ACTIVE, IDLE, CLOSED }

    /** Result of admission: either a warm resource reservation or a new-environment permit. */
    public record Admission<T>(T reusable, Permit permit) {
        private static <T> Admission<T> reused(T value) {
            return new Admission<>(value, null);
        }

        private static <T> Admission<T> permitted(Permit permit) {
            return new Admission<>(null, permit);
        }

        public boolean reused() {
            return reusable != null;
        }
    }

    private static final class PermitImpl implements Permit {
        private final LambdaEnvironmentLimiter owner;
        private final String key;
        private final AtomicBoolean closed = new AtomicBoolean();
        private PermitState state = PermitState.ACTIVE;
        /** Incremented for every activation so a stale release cannot change a newer lease. */
        private long generation;

        private PermitImpl(LambdaEnvironmentLimiter owner, String key) {
            this.owner = owner;
            this.key = key;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.release(this);
            }
        }
    }

    /**
     * Physical admission readback. {@code total} is always {@code active + idle}; queued is
     * intentionally separate because waiters do not own a physical permit yet.
     */
    public record Status(int configuredLimit,
                         int total,
                         int active,
                         int idle,
                         int queued,
                         int available,
                         long granted,
                         long timedOut,
                         long interrupted,
                         boolean closed) {
        public boolean bounded() {
            return configuredLimit > UNBOUNDED;
        }
    }

    /** Physical admission readback scoped to one immutable execution-environment identity. */
    public record KeyStatus(String environmentKey, int total, int active, int idle, int queued) {
    }

    public static class AdmissionTimeoutException extends RuntimeException {
        private final String environmentKey;
        private final int configuredLimit;

        public AdmissionTimeoutException(String environmentKey, int configuredLimit, long waitNanos) {
            super("Timed out waiting for a Lambda execution environment for " + environmentKey
                    + " after " + TimeUnit.NANOSECONDS.toMillis(waitNanos) + "ms"
                    + " (configured limit=" + configuredLimit + ")");
            this.environmentKey = environmentKey;
            this.configuredLimit = configuredLimit;
        }

        public String environmentKey() {
            return environmentKey;
        }

        public int configuredLimit() {
            return configuredLimit;
        }
    }

    public static class AdmissionInterruptedException extends RuntimeException {
        private final String environmentKey;

        public AdmissionInterruptedException(String environmentKey, Throwable cause) {
            super("Interrupted waiting for a Lambda execution environment for " + environmentKey, cause);
            this.environmentKey = environmentKey;
        }

        public String environmentKey() {
            return environmentKey;
        }
    }

    public static class AdmissionClosedException extends RuntimeException {
        private final String environmentKey;

        public AdmissionClosedException(String environmentKey) {
            super("Lambda execution-environment admission is closed for " + environmentKey);
            this.environmentKey = environmentKey;
        }

        public String environmentKey() {
            return environmentKey;
        }
    }
}
