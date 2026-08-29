package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ContainerTeardown;
import io.github.hectorvent.floci.services.lambda.launcher.ContainerHandle;
import io.github.hectorvent.floci.services.lambda.launcher.LambdaRuntimeLauncher;
import io.github.hectorvent.floci.services.lambda.model.ContainerState;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages a pool of warm Lambda containers per function.
 *
 * Two modes controlled by {@code emulator.services.lambda.ephemeral}:
 *  - {@code false} (default): containers are reused across invocations and evicted
 *    after {@code container-idle-timeout-seconds} of inactivity.
 *  - {@code true}: each invocation gets a fresh container that is stopped immediately
 *    after the invocation completes.
 *
 * <p>When a physical-environment cap is configured, each launched handle owns one close-once
 * admission permit for its entire lifetime. The permit moves between active and idle state as
 * the handle is reused and is released only when the container is stopped, drained, or fails.
 */
@ApplicationScoped
public class WarmPool implements ContainerTeardown {

    private static final Logger LOG = Logger.getLogger(WarmPool.class);

    private static final int DEFAULT_MAX_POOL_SIZE = Math.max(4, Runtime.getRuntime().availableProcessors());

    private final LambdaRuntimeLauncher lambdaRuntimeLauncher;
    private final EmulatorConfig config;
    private final LambdaEnvironmentLimiter environmentLimiter;
    private final int maxPoolSizePerFunction;
    private final ConcurrentHashMap<FunctionPoolKey, PoolState> poolStates = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ContainerHandle, Lease> activeLeases = new ConcurrentHashMap<>();
    /** Permits retained for handles whose teardown has not yet been confirmed. */
    private final ConcurrentHashMap<ContainerHandle, LambdaEnvironmentLimiter.Permit> pendingRetirements
            = new ConcurrentHashMap<>();
    private final AtomicInteger idleContainerCount = new AtomicInteger();
    private final AtomicLong idleSequence = new AtomicLong();
    private final ExecutorService retirementExecutor = Executors.newCachedThreadPool(
            r -> { Thread t = new Thread(r, "warm-pool-retirement"); t.setDaemon(true); return t; });
    private final ScheduledExecutorService evictionScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "warm-pool-evictor"); t.setDaemon(true); return t; });
    /** Serializes the terminal draining transition with warm-pool publication. */
    private final Object lifecycleLock = new Object();
    /** Once set, every active release is a retirement, never a warm-pool publication. */
    private volatile boolean draining;

    private record FunctionPoolKey(String accountId, String region, String functionName) {
    }

    private record IdleLease(ContainerHandle handle,
                             LambdaEnvironmentLimiter.Permit environmentPermit,
                             long epoch,
                             long generation,
                             long sequence) {
    }

    private static final class PoolState {
        private final Map<String, ArrayDeque<IdleLease>> idleByEnvironment = new HashMap<>();
        private final Map<String, Long> epochByEnvironment = new HashMap<>();
    }

    private record Lease(PoolState poolState,
                         long epoch,
                         String environmentKey,
                         LambdaEnvironmentLimiter.Permit environmentPermit,
                         long generation) {
    }

    @Inject
    public WarmPool(LambdaRuntimeLauncher lambdaRuntimeLauncher,
                    EmulatorConfig config,
                    LambdaEnvironmentLimiter environmentLimiter) {
        this.lambdaRuntimeLauncher = lambdaRuntimeLauncher;
        this.config = config;
        this.environmentLimiter = environmentLimiter;
        this.maxPoolSizePerFunction = maxPoolSize(config);
    }

    /** Constructor retained for focused tests and embedders that do not use CDI. */
    public WarmPool(LambdaRuntimeLauncher lambdaRuntimeLauncher, EmulatorConfig config) {
        this(lambdaRuntimeLauncher, config, new LambdaEnvironmentLimiter(config));
    }

    /** Package-private constructor for testing (empty pool, no containers to drain). */
    WarmPool() {
        this.lambdaRuntimeLauncher = null;
        this.config = null;
        this.environmentLimiter = new LambdaEnvironmentLimiter();
        this.maxPoolSizePerFunction = DEFAULT_MAX_POOL_SIZE;
    }

    @PostConstruct
    void init() {
        if (config == null) {
            return;
        }

        int idleTimeout = config.services().lambda().containerIdleTimeoutSeconds();
        if (!config.services().lambda().ephemeral() && idleTimeout > 0) {
            // Check for idle containers every 30 seconds (or half the timeout, whichever is less)
            long checkInterval = Math.min(30, idleTimeout / 2 + 1);
            evictionScheduler.scheduleAtFixedRate(this::evictIdleContainers,
                    checkInterval, checkInterval, TimeUnit.SECONDS);
            LOG.infov("Warm pool idle eviction enabled: timeout={0}s, check interval={1}s",
                    idleTimeout, checkInterval);
        } else if (config.services().lambda().ephemeral()) {
            LOG.infov("Lambda containers running in ephemeral mode (destroyed after each invocation)");
        }

    }

    /**
     * Invoked from EmulatorLifecycle.onStop for a deterministic drain during the
     * ShutdownEvent phase; the {@code @PreDestroy} below stays as an idempotent fallback.
     * This replaces the previous raw JVM shutdown hook, which raced the Quarkus-managed
     * shutdown sequence.
     */
    @Override
    public void stopManagedContainers() {
        synchronized (lifecycleLock) {
            draining = true;
        }
        environmentLimiter.close();
        drainAll();
    }

    @PreDestroy
    void shutdown() {
        synchronized (lifecycleLock) {
            draining = true;
        }
        evictionScheduler.shutdownNow();
        environmentLimiter.close();
        drainAll();
        retirementExecutor.shutdownNow();
    }

    /**
     * Acquires a container for the given function.
     * In ephemeral mode always cold-starts a new container.
     * Otherwise returns a warm container from the pool, or cold-starts a new one.
     */
    public ContainerHandle acquire(LambdaFunction fn) {
        String environmentKey = executionEnvironmentKey(fn);
        boolean ephemeral = config != null && config.services().lambda().ephemeral();
        FunctionPoolKey poolKey = functionPoolKey(fn);
        PoolState poolState = poolStates.computeIfAbsent(poolKey, ignored -> new PoolState());

        while (true) {
            LambdaEnvironmentLimiter.Admission<IdleLease> admission = environmentLimiter.acquire(
                    environmentKey,
                    () -> ephemeral ? null : takeIdleOrRetire(poolState, environmentKey));

            if (admission.reused()) {
                IdleLease idleLease = admission.reusable();
                ContainerHandle handle = idleLease.handle();
                boolean current;
                synchronized (poolState) {
                    current = idleLease.epoch() == poolState.epochByEnvironment
                            .getOrDefault(environmentKey, 0L);
                }
                if (!current) {
                    stopAndRelease(idleLease);
                    continue;
                }
                try {
                    if (!isUsableWarmHandle(handle, fn)) {
                        stopAndRelease(idleLease);
                        continue;
                    }
                } catch (RuntimeException | Error e) {
                    stopAndRelease(idleLease);
                    throw e;
                }
                Lease lease = null;
                try {
                    long generation = environmentLimiter.markActive(idleLease.environmentPermit());
                    lease = new Lease(poolState, idleLease.epoch(), environmentKey,
                            idleLease.environmentPermit(), generation);
                    activeLeases.put(handle, lease);
                    handle.setState(ContainerState.BUSY);
                    LOG.debugv("Reusing warm container for function: {0}", fn.getFunctionName());
                    return handle;
                } catch (RuntimeException | Error e) {
                    if (lease != null) {
                        activeLeases.remove(handle, lease);
                    }
                    stopAndRelease(idleLease);
                    throw e;
                }
            }

            LambdaEnvironmentLimiter.Permit environmentPermit = admission.permit();
            long leaseEpoch;
            synchronized (poolState) {
                leaseEpoch = poolState.epochByEnvironment.computeIfAbsent(environmentKey, ignored -> 0L);
            }
            ContainerHandle handle = null;
            Lease lease = null;
            try {
                LOG.debugv(ephemeral ? "Ephemeral start for function: {0}" : "Cold start for function: {0}",
                        fn.getFunctionName());
                handle = lambdaRuntimeLauncher.launch(fn);
                long generation = environmentLimiter.markActive(environmentPermit);
                lease = new Lease(poolState, leaseEpoch, environmentKey, environmentPermit, generation);
                activeLeases.put(handle, lease);
                handle.setState(ContainerState.BUSY);
                return handle;
            } catch (RuntimeException | Error e) {
                // A launcher may have created a handle before a later setup operation failed.
                // Stop that handle as well as releasing its lifetime permit so a partial launch
                // cannot remain live outside the pool.
                if (lease != null) {
                    activeLeases.remove(handle, lease);
                }
                if (handle != null) {
                    stopAndRelease(handle, environmentPermit);
                } else {
                    environmentPermit.close();
                }
                throw e;
            }
        }
    }

    /**
     * Removes one matching idle handle while the limiter serializes admission. If the cap is full
     * and no matching handle exists, the oldest unrelated idle lease is removed from the pool and
     * retired asynchronously. Its permit remains owned until teardown confirms that the handle
     * is gone, so the limiter never grants a slot for a container that may still be running.
     */
    private IdleLease takeIdleOrRetire(PoolState poolState, String environmentKey) {
        synchronized (poolState) {
            ArrayDeque<IdleLease> idle = poolState.idleByEnvironment.get(environmentKey);
            IdleLease lease = idle == null ? null : idle.pollFirst();
            if (lease != null) {
                idleContainerCount.decrementAndGet();
                if (idle.isEmpty()) {
                    poolState.idleByEnvironment.remove(environmentKey);
                }
            }
            if (lease != null) {
                return lease;
            }
        }
        if (environmentLimiter.bounded()
                && environmentLimiter.totalCount() >= environmentLimiter.configuredLimit()) {
            IdleLease victim = removeOldestIdle(environmentKey);
            if (victim != null) {
                LOG.infov("Retiring idle container {0} to admit a new Lambda environment",
                        victim.handle().getContainerId());
                // This callback executes under the limiter's admission lock. Docker teardown can
                // block well beyond the admission budget, so never perform it inline here. The
                // permit is closed by the retirement task only after stop has returned (or a
                // failed stop has been reconciled as no longer alive).
                scheduleRetirement(victim);
            }
        }
        return null;
    }

    private boolean isUsableWarmHandle(ContainerHandle handle, LambdaFunction fn) {
        // A container whose extension reported a fatal error is still *running*, so the
        // liveness probe alone would hand it back out. Skip it for the same reason a dead
        // one is skipped: it can no longer serve invocations correctly.
        boolean faulted;
        faulted = handle.isFaulted();
        if (faulted) {
            LOG.infov("Discarding pooled container {0} for function {1}: an extension reported a fatal error",
                    handle.getContainerId(), fn.getFunctionName());
            return false;
        }
        boolean alive;
        alive = lambdaRuntimeLauncher.isAlive(handle);
        if (!alive) {
            LOG.infov("Discarding dead pooled container {0} for function {1}",
                    handle.getContainerId(), fn.getFunctionName());
        }
        return alive;
    }

    /**
     * Retires the oldest idle environment when the physical cap is full. This is deliberately
     * global: an idle environment for another function still owns one of the finite Docker
     * slots, and releasing that slot lets the queued invocation cold-start its own identity.
     */
    private IdleLease removeOldestIdle(String preserveEnvironmentKey) {
        while (true) {
            PoolState victimState = null;
            String victimKey = null;
            IdleLease victim = null;
            for (var entry : poolStates.entrySet()) {
                PoolState state = entry.getValue();
                synchronized (state) {
                    for (var idleEntry : state.idleByEnvironment.entrySet()) {
                        if (idleEntry.getKey().equals(preserveEnvironmentKey)) {
                            continue;
                        }
                        IdleLease candidate = idleEntry.getValue().peekLast();
                        if (candidate != null && (victim == null
                                || candidate.sequence() < victim.sequence())) {
                            victimState = state;
                            victimKey = idleEntry.getKey();
                            victim = candidate;
                        }
                    }
                }
            }
            if (victim == null) {
                return null;
            }
            synchronized (victimState) {
                ArrayDeque<IdleLease> idle = victimState.idleByEnvironment.get(victimKey);
                if (idle == null || !idle.remove(victim)) {
                    continue;
                }
                if (idle.isEmpty()) {
                    victimState.idleByEnvironment.remove(victimKey);
                }
                idleContainerCount.decrementAndGet();
            }
            return victim;
        }
    }

    /**
     * Returns a container after an invocation completes.
     * In ephemeral mode the container is stopped immediately.
     * Otherwise it is returned to the warm pool.
     */
    public void release(ContainerHandle handle) {
        Lease lease = activeLeases.remove(handle);
        boolean ephemeral = config != null && config.services().lambda().ephemeral();
        // Shutdown closes admission and drains already-idle handles, but an invocation can still
        // finish afterward. The draining flag is checked again under the pool-state lock below so
        // this late release can never publish a live handle after drainAll() has taken its snapshot.
        if (draining) {
            stopAndRelease(handle, lease);
            return;
        }
        // An extension reporting init/exit error is fatal to the execution environment in real
        // AWS. RuntimeApiServer already refuses new work at that point; the container is torn down
        // here rather than at fault time so the invocation that was in flight when the extension
        // failed still completes normally through the runtime.
        boolean faulted;
        try {
            faulted = handle.isFaulted();
        } catch (RuntimeException | Error e) {
            stopAndRelease(handle, lease);
            throw e;
        }
        if (faulted) {
            LOG.infov("Retiring container {0} for function {1}: an extension reported a fatal error",
                    handle.getContainerId(), handle.getFunctionName());
            stopAndRelease(handle, lease);
            return;
        }
        if (ephemeral || handle.isHotReload()) {
            LOG.debugv("{0}: stopping container {1} after invocation",
                    handle.isHotReload() ? "Hot-reload" : "Ephemeral", handle.getContainerId());
            stopAndRelease(handle, lease);
            return;
        }

        if (lease == null) {
            LOG.warnv("Container {0} for function {1} has no active warm-pool lease; stopping it",
                    handle.getContainerId(), handle.getFunctionName());
            stopQuietly(handle);
            return;
        }

        boolean stale;
        boolean returned;
        boolean releaseDuringDrain;
        // Hold lifecycleLock while publishing the idle handle. A concurrent managed drain either
        // wins first (and this release retires directly) or snapshots the newly published handle
        // after setting draining=true; it cannot miss a late release between those states.
        synchronized (lifecycleLock) {
            releaseDuringDrain = draining;
            if (releaseDuringDrain) {
                stale = false;
                returned = false;
            } else {
                synchronized (lease.poolState()) {
                    stale = lease.epoch() != lease.poolState().epochByEnvironment
                            .getOrDefault(lease.environmentKey(), 0L);
                    returned = !stale && idleSize(lease.poolState()) < maxPoolSizePerFunction;
                    if (returned) {
                        handle.setState(ContainerState.WARM);
                        handle.touchLastUsed();
                        lease.poolState().idleByEnvironment
                                .computeIfAbsent(lease.environmentKey(), ignored -> new ArrayDeque<>())
                                .addFirst(new IdleLease(handle, lease.environmentPermit(), lease.epoch(),
                                        lease.generation(),
                                        idleSequence.incrementAndGet()));
                        idleContainerCount.incrementAndGet();
                    }
                }
            }
        }
        if (releaseDuringDrain) {
            LOG.debugv("Warm pool is draining; stopping late container {0}", handle.getContainerId());
            stopAndRelease(handle, lease);
        } else if (stale) {
            LOG.debugv("Pool was invalidated while container {0} was busy; stopping it",
                    handle.getContainerId());
            stopAndRelease(handle, lease);
        } else if (returned) {
            if (environmentLimiter.markIdle(lease.environmentPermit(), lease.generation())) {
                environmentLimiter.signalReusable(lease.environmentKey());
                LOG.debugv("Released container back to pool for function: {0}", handle.getFunctionName());
            } else {
                // A concurrent acquire may have reclaimed the just-published handle before this
                // release updated the physical readback. Its generation is newer, so leave the
                // permit active and do not wake unrelated waiters for a resource they cannot use.
                LOG.debugv("Container {0} was reclaimed before its release became idle",
                        handle.getContainerId());
            }
        } else {
            LOG.debugv("Pool full for function {0}, stopping excess container", handle.getFunctionName());
            stopAndRelease(handle, lease);
        }
    }

    /**
     * Pushes a code update to all warm containers in the pool for the given function.
     * In this implementation, we drain the containers to force a fresh start with new code.
     */
    public void pushCodeUpdate(LambdaFunction fn) {
        LOG.infov("Reactive S3 Sync: invalidating warm pool for function {0} to pick up new code",
                fn.getFunctionName());
        drainEnvironment(fn);
    }

    /**
     * Stops and removes a single container that is no longer usable (e.g. after a timeout).
     * The container must have already been acquired (removed from the pool) so only a
     * stop is needed — no pool bookkeeping required.
     */
    public void destroyHandle(ContainerHandle handle) {
        LOG.debugv("Destroying timed-out container {0} for function {1}",
                handle.getContainerId(), handle.getFunctionName());
        Lease lease = activeLeases.remove(handle);
        stopAndRelease(handle, lease);
    }

    /**
     * Stops and removes warm containers for one immutable execution environment, such as
     * {@code $LATEST}. Published versions keep their independently keyed warm containers.
     */
    public void drainEnvironment(LambdaFunction fn) {
        String environmentKey = executionEnvironmentKey(fn);
        FunctionPoolKey poolKey = functionPoolKey(fn);
        PoolState poolState = poolStates.computeIfAbsent(poolKey, ignored -> new PoolState());
        List<IdleLease> toStop;
        synchronized (poolState) {
            poolState.epochByEnvironment.merge(environmentKey, 1L, Long::sum);
            ArrayDeque<IdleLease> idle = poolState.idleByEnvironment.remove(environmentKey);
            toStop = idle == null ? List.of() : new ArrayList<>(idle);
            idleContainerCount.addAndGet(-toStop.size());
        }
        LOG.infov("Draining {0} container(s) for Lambda environment: {1}",
                toStop.size(), environmentKey);
        stopInParallel(toStop);
    }

    /**
     * Stops and removes all warm containers for every environment of the given function.
     * Called on function deletion and emulator shutdown.
     */
    public void drainFunction(String functionName) {
        List<IdleLease> toStop = new ArrayList<>();
        for (var entry : poolStates.entrySet()) {
            if (!entry.getKey().functionName().equals(functionName)) {
                continue;
            }
            toStop.addAll(drainPoolState(entry.getValue()));
        }
        LOG.infov("Draining {0} container(s) for function: {1}", toStop.size(), functionName);
        stopInParallel(toStop);
    }

    /**
     * Stops and invalidates every warm environment for one account/region/function identity.
     * The name-only overload above remains for callers that intentionally drain all accounts.
     */
    public void drainFunction(LambdaFunction fn) {
        PoolState poolState = poolStates.computeIfAbsent(functionPoolKey(fn), ignored -> new PoolState());
        List<IdleLease> toStop = drainPoolState(poolState);
        LOG.infov("Draining {0} container(s) for function: {1}", toStop.size(), fn.getFunctionName());
        stopInParallel(toStop);
    }

    private void stopInParallel(List<IdleLease> handles) {
        if (handles.isEmpty()) {
            return;
        }
        int parallelism = Math.min(handles.size(), 16);
        ExecutorService pool = Executors.newFixedThreadPool(parallelism,
                r -> { Thread t = new Thread(r, "warm-pool-drainer"); t.setDaemon(true); return t; });
        try {
            List<Future<?>> futures = new ArrayList<>(handles.size());
            for (IdleLease lease : handles) {
                futures.add(pool.submit(() -> stopAndRelease(lease)));
            }
            for (Future<?> f : futures) {
                try {
                    f.get(15, TimeUnit.SECONDS);
                } catch (Exception e) {
                    LOG.warnv("Drain task did not finish cleanly: {0}", e.getMessage());
                }
            }
        } finally {
            pool.shutdownNow();
        }
    }

    private void drainAll() {
        for (PoolState poolState : new ArrayList<>(poolStates.values())) {
            stopInParallel(drainPoolState(poolState));
        }
    }

    private void evictIdleContainers() {
        if (config == null) {
            return;
        }
        long idleTimeoutMs = config.services().lambda().containerIdleTimeoutSeconds() * 1000L;
        long now = System.currentTimeMillis();

        for (var entry : poolStates.entrySet()) {
            String functionName = entry.getKey().functionName();
            PoolState poolState = entry.getValue();
            List<IdleLease> toEvict = new ArrayList<>();

            synchronized (poolState) {
                for (ArrayDeque<IdleLease> idle : poolState.idleByEnvironment.values()) {
                    idle.removeIf(lease -> {
                        ContainerHandle handle = lease.handle();
                        if (handle.getState() == ContainerState.WARM
                                && (now - handle.getLastUsedMs()) >= idleTimeoutMs) {
                            toEvict.add(lease);
                            return true;
                        }
                        return false;
                    });
                }
                poolState.idleByEnvironment.values().removeIf(ArrayDeque::isEmpty);
                idleContainerCount.addAndGet(-toEvict.size());
            }

            if (!toEvict.isEmpty()) {
                LOG.infov("Evicting {0} idle container(s) for function: {1}", toEvict.size(), functionName);
                for (IdleLease lease : toEvict) {
                    stopAndRelease(lease);
                }
            }
        }
    }

    private static String executionEnvironmentKey(LambdaFunction fn) {
        String functionArn = fn.getFunctionArn();
        if (functionArn != null && !functionArn.isBlank()) {
            return functionArn;
        }
        return accountId(fn) + ":" + fn.getFunctionName() + ":" + fn.getVersion();
    }

    private static FunctionPoolKey functionPoolKey(LambdaFunction fn) {
        return new FunctionPoolKey(accountId(fn), regionId(fn), fn.getFunctionName());
    }

    private static String regionId(LambdaFunction fn) {
        String functionArn = fn.getFunctionArn();
        if (functionArn != null && !functionArn.isBlank()) {
            String[] segments = functionArn.split(":", 6);
            if (segments.length >= 4 && !segments[3].isBlank()) {
                return segments[3];
            }
        }
        return "unknown";
    }

    private static String accountId(LambdaFunction fn) {
        String accountId = fn.getAccountId();
        if (accountId != null && !accountId.isBlank()) {
            return accountId;
        }
        String functionArn = fn.getFunctionArn();
        if (functionArn != null && !functionArn.isBlank()) {
            String[] segments = functionArn.split(":", 6);
            if (segments.length >= 5 && !segments[4].isBlank()) {
                return segments[4];
            }
        }
        return "000000000000";
    }

    private List<IdleLease> drainPoolState(PoolState poolState) {
        List<IdleLease> toStop = new ArrayList<>();
        synchronized (poolState) {
            poolState.epochByEnvironment.replaceAll((ignored, epoch) -> epoch + 1);
            poolState.idleByEnvironment.values().forEach(toStop::addAll);
            poolState.idleByEnvironment.clear();
            idleContainerCount.addAndGet(-toStop.size());
        }
        return toStop;
    }

    /** Status/readback for physical environment admission. */
    public LambdaEnvironmentLimiter.Status status() {
        return environmentLimiter.status();
    }

    /** Number of currently idle warm containers retained by this pool. */
    public int idleContainerCount() {
        return idleContainerCount.get();
    }

    private static int maxPoolSize(EmulatorConfig config) {
        if (config == null || config.services() == null || config.services().lambda() == null) {
            return DEFAULT_MAX_POOL_SIZE;
        }
        var configured = config.services().lambda().maxPhysicalEnvironments();
        if (configured == null || configured.isEmpty() || configured.getAsInt() <= 0) {
            return DEFAULT_MAX_POOL_SIZE;
        }
        return Math.min(DEFAULT_MAX_POOL_SIZE, configured.getAsInt());
    }

    private static int idleSize(PoolState poolState) {
        int size = 0;
        for (ArrayDeque<IdleLease> idle : poolState.idleByEnvironment.values()) {
            size += idle.size();
        }
        return size;
    }

    /**
     * Schedules a retirement selected while the limiter lock is held. Teardown is intentionally
     * outside that lock: Docker and Kubernetes stop operations can block, while other waiters must
     * continue to observe their own bounded deadlines.
     */
    private void scheduleRetirement(IdleLease lease) {
        try {
            retirementExecutor.execute(() -> stopAndRelease(lease));
        } catch (RejectedExecutionException e) {
            // Retain the permit when the executor is already shutting down. Admission is closed
            // during the only normal path to this branch, and retaining ownership is safer than
            // allowing a still-running handle to fall outside the physical cap.
            LOG.warnv("Could not schedule retirement for container {0}; retaining its physical permit",
                    lease.handle().getContainerId());
        }
    }

    private void stopAndRelease(IdleLease lease) {
        stopAndRelease(lease.handle(), lease.environmentPermit());
    }

    private void stopAndRelease(ContainerHandle handle, LambdaEnvironmentLimiter.Permit permit) {
        if (permit == null) {
            stopQuietly(handle);
            return;
        }
        if (stopConfirmed(handle)) {
            pendingRetirements.remove(handle, permit);
            permit.close();
        } else {
            retainAndRetryRetirement(handle, permit);
        }
    }

    private void stopAndRelease(ContainerHandle handle, Lease lease) {
        if (lease == null) {
            stopQuietly(handle);
            return;
        }
        stopAndRelease(handle, lease.environmentPermit());
    }

    /**
     * Teardown is confirmed only when the launcher reports the handle is no longer alive. The
     * launcher interface is void-returning and concrete implementations may absorb Docker/API
     * deletion failures, so a normal return alone cannot justify releasing a lifetime permit.
     * A failed or inconclusive probe retains ownership and is retried asynchronously.
     */
    private boolean stopConfirmed(ContainerHandle handle) {
        try {
            lambdaRuntimeLauncher.stop(handle);
        } catch (Exception stopFailure) {
            LOG.warnv("Error stopping container {0}: {1}", handle.getContainerId(),
                    stopFailure.getMessage());
            // Continue to the liveness probe: a stop that throws can still have removed the
            // handle, in which case the lifetime permit is safe to release.
        }
        try {
            boolean alive = lambdaRuntimeLauncher.isAlive(handle);
            if (!alive) {
                return true;
            }
            LOG.warnv("Container {0} remains alive after teardown; retaining its physical permit",
                    handle.getContainerId());
        } catch (Exception probeFailure) {
            LOG.warnv("Could not reconcile container {0} after teardown; retaining its physical permit: {1}",
                    handle.getContainerId(), probeFailure.getMessage());
        }
        return false;
    }

    private void retainAndRetryRetirement(ContainerHandle handle, LambdaEnvironmentLimiter.Permit permit) {
        if (pendingRetirements.putIfAbsent(handle, permit) != null) {
            return;
        }
        try {
            retirementExecutor.execute(() -> retryRetirement(handle, permit));
        } catch (RejectedExecutionException e) {
            // Keep the entry and permit. A later reconciler/lifecycle pass can inspect the
            // ownership; freeing it here would make the physical cap a lie.
            LOG.warnv("Could not schedule retirement retry for container {0}; retaining its physical permit",
                    handle.getContainerId());
        }
    }

    private void retryRetirement(ContainerHandle handle, LambdaEnvironmentLimiter.Permit permit) {
        try {
            while (pendingRetirements.get(handle) == permit) {
                if (stopConfirmed(handle)) {
                    if (pendingRetirements.remove(handle, permit)) {
                        permit.close();
                    }
                    return;
                }
                Thread.sleep(250);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Leave ownership in pendingRetirements. shutdownNow() may interrupt this worker,
            // but it must not turn an unconfirmed teardown into available capacity.
        }
    }

    private void stopQuietly(ContainerHandle handle) {
        try {
            lambdaRuntimeLauncher.stop(handle);
        } catch (Exception e) {
            LOG.warnv("Error stopping container {0}: {1}", handle.getContainerId(), e.getMessage());
        }
    }
}
