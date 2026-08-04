package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ContainerTeardown;
import io.github.hectorvent.floci.services.lambda.launcher.ContainerHandle;
import io.github.hectorvent.floci.services.lambda.launcher.ContainerLauncher;
import io.github.hectorvent.floci.services.lambda.launcher.LambdaExecutionEnvironmentId;
import io.github.hectorvent.floci.services.lambda.model.ContainerState;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages a pool of warm Lambda containers per function.
 *
 * Two modes controlled by {@code emulator.services.lambda.ephemeral}:
 *  - {@code false} (default): containers are reused across invocations and evicted
 *    after {@code container-idle-timeout-seconds} of inactivity.
 *  - {@code true}: each invocation gets a fresh container that is stopped immediately
 *    after the invocation completes.
 */
@ApplicationScoped
public class WarmPool implements ContainerTeardown {

    private static final Logger LOG = Logger.getLogger(WarmPool.class);

    private static final int DEFAULT_MAX_POOL_SIZE = Math.max(4, Runtime.getRuntime().availableProcessors());

    private final ContainerLauncher containerLauncher;
    private final EmulatorConfig config;
    private final int maxPoolSizePerFunction;
    private final ConcurrentHashMap<FunctionIdentity, FunctionPool> pool = new ConcurrentHashMap<>();
    private final ScheduledExecutorService evictionScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> { Thread t = new Thread(r, "warm-pool-evictor"); t.setDaemon(true); return t; });

    @Inject
    public WarmPool(ContainerLauncher containerLauncher, EmulatorConfig config) {
        this.containerLauncher = containerLauncher;
        this.config = config;
        this.maxPoolSizePerFunction = DEFAULT_MAX_POOL_SIZE;
    }

    /** Package-private constructor for testing (empty pool, no containers to drain). */
    WarmPool() {
        this.containerLauncher = null;
        this.config = null;
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
        drainAll();
    }

    @PreDestroy
    void shutdown() {
        evictionScheduler.shutdownNow();
        drainAll();
    }

    /**
     * Acquires a container for the given function.
     * In ephemeral mode always cold-starts a new container.
     * Otherwise returns a warm container from the pool, or cold-starts a new one.
     */
    public ContainerHandle acquire(LambdaFunction fn) {
        boolean ephemeral = config != null && config.services().lambda().ephemeral();
        ContainerHandle handle = null;
        LambdaExecutionEnvironmentId environmentId = LambdaExecutionEnvironmentId.from(fn);
        FunctionPool functionPool = pool.computeIfAbsent(
                FunctionIdentity.from(environmentId), ignored -> new FunctionPool());
        long generation;

        synchronized (functionPool) {
            if (functionPool.environmentId == null && !functionPool.retired) {
                functionPool.environmentId = environmentId;
            }
            // Management-plane create/update/delete owns which revision is current. A delayed
            // invocation that still carries an older function snapshot may run, but it must not
            // roll the pool back or become reusable after it completes.
            generation = environmentId.equals(functionPool.environmentId)
                    ? functionPool.generation : -1;
        }

        if (!ephemeral && generation >= 0) {
            // Skip pooled handles whose container died out-of-band — otherwise the
            // caller would wait the full Lambda function timeout.
            while (true) {
                ContainerHandle candidate;
                synchronized (functionPool) {
                    candidate = functionPool.queue.pollFirst();
                }
                if (candidate == null) {
                    break;
                }
                // A container whose extension reported a fatal error is still *running*, so the
                // liveness probe alone would hand it back out. Skip it for the same reason a dead
                // one is skipped: it can no longer serve invocations correctly.
                if (candidate.isFaulted()) {
                    LOG.infov("Discarding pooled container {0} for function {1}: an extension reported a fatal error",
                            candidate.getContainerId(), fn.getFunctionName());
                    stopQuietly(candidate);
                    continue;
                }
                if (containerLauncher.isAlive(candidate)) {
                    handle = candidate;
                    break;
                }
                LOG.infov("Discarding dead pooled container {0} for function {1}",
                        candidate.getContainerId(), fn.getFunctionName());
                stopQuietly(candidate);
            }
        }

        if (handle == null) {
            LOG.debugv(ephemeral ? "Ephemeral start for function: {0}" : "Cold start for function: {0}",
                    fn.getFunctionName());
            handle = containerLauncher.launch(fn, environmentId, generation);
        } else {
            LOG.debugv("Reusing warm container for function: {0}", fn.getFunctionName());
        }
        handle.setState(ContainerState.BUSY);
        return handle;
    }

    /**
     * Returns a container after an invocation completes.
     * In ephemeral mode the container is stopped immediately.
     * Otherwise it is returned to the warm pool.
     */
    public void release(ContainerHandle handle) {
        boolean ephemeral = config != null && config.services().lambda().ephemeral();
        // An extension reporting an init/exit error is fatal to the execution environment in real
        // AWS. RuntimeApiServer already refuses new work at that point; the container is torn down
        // here rather than at fault time so the invocation that was in flight when the extension
        // failed still completes normally through the runtime.
        if (handle.isFaulted()) {
            LOG.infov("Retiring container {0} for function {1}: an extension reported a fatal error",
                    handle.getContainerId(), handle.getFunctionName());
            stopQuietly(handle);
            return;
        }
        if (ephemeral || handle.isHotReload()) {
            LOG.debugv("{0}: stopping container {1} after invocation",
                    handle.isHotReload() ? "Hot-reload" : "Ephemeral", handle.getContainerId());
            stopQuietly(handle);
            return;
        }

        FunctionIdentity functionIdentity = FunctionIdentity.from(handle.getExecutionEnvironmentId());
        FunctionPool functionPool = pool.get(functionIdentity);
        if (functionPool == null) {
            stopQuietly(handle);
            return;
        }
        boolean returned;
        synchronized (functionPool) {
            returned = handle.getPoolGeneration() == functionPool.generation
                    && handle.getExecutionEnvironmentId().equals(functionPool.environmentId)
                    && functionPool.queue.size() < maxPoolSizePerFunction;
            if (returned) {
                handle.setState(ContainerState.WARM);
                handle.touchLastUsed();
                functionPool.queue.addFirst(handle);
            }
        }
        if (returned) {
            LOG.debugv("Released container back to pool for function: {0}", handle.getFunctionName());
        } else {
            LOG.debugv("Container for function {0} belongs to a retired or full pool; stopping it",
                    handle.getFunctionName());
            stopQuietly(handle);
        }
    }

    /**
     * Pushes a code update to all warm containers in the pool for the given function.
     * In this implementation, we drain the containers to force a fresh start with new code.
     */
    public void pushCodeUpdate(LambdaFunction fn) {
        LOG.infov("Reactive S3 Sync: invalidating warm pool for function {0} to pick up new code",
                fn.getFunctionName());
        drainFunction(fn);
    }

    /**
     * Stops and removes a single container that is no longer usable (e.g. after a timeout).
     * The container must have already been acquired (removed from the pool) so only a
     * stop is needed — no pool bookkeeping required.
     */
    public void destroyHandle(ContainerHandle handle) {
        LOG.debugv("Destroying timed-out container {0} for function {1}",
                handle.getContainerId(), handle.getFunctionName());
        stopQuietly(handle);
    }

    /**
     * Stops and removes all warm containers for the given function.
     * Called on function delete or code update.
     */
    public void drainFunction(LambdaFunction fn) {
        LambdaExecutionEnvironmentId environmentId = LambdaExecutionEnvironmentId.from(fn);
        FunctionIdentity identity = FunctionIdentity.from(environmentId);
        FunctionPool functionPool = pool.computeIfAbsent(identity, ignored -> new FunctionPool());
        List<ContainerHandle> toStop;
        synchronized (functionPool) {
            functionPool.generation++;
            functionPool.environmentId = environmentId;
            functionPool.retired = false;
            toStop = new ArrayList<>(functionPool.queue);
            functionPool.queue.clear();
        }
        LOG.infov("Draining {0} container(s) for function: {1}", toStop.size(),
                environmentId.qualifiedFunctionArn());
        stopInParallel(toStop);
    }

    /** Retires a deleted function identity without allowing a delayed invocation to reactivate it. */
    public void retireFunction(LambdaFunction fn) {
        LambdaExecutionEnvironmentId environmentId = LambdaExecutionEnvironmentId.from(fn);
        FunctionPool functionPool = pool.computeIfAbsent(
                FunctionIdentity.from(environmentId), ignored -> new FunctionPool());
        List<ContainerHandle> toStop;
        synchronized (functionPool) {
            functionPool.generation++;
            functionPool.environmentId = null;
            functionPool.retired = true;
            toStop = new ArrayList<>(functionPool.queue);
            functionPool.queue.clear();
        }
        stopInParallel(toStop);
    }

    private void stopInParallel(List<ContainerHandle> handles) {
        if (handles.isEmpty()) return;
        int parallelism = Math.min(handles.size(), 16);
        ExecutorService pool = Executors.newFixedThreadPool(parallelism,
                r -> { Thread t = new Thread(r, "warm-pool-drainer"); t.setDaemon(true); return t; });
        try {
            List<Future<?>> futures = new ArrayList<>(handles.size());
            for (ContainerHandle handle : handles) {
                futures.add(pool.submit(() -> stopQuietly(handle)));
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
        for (FunctionPool functionPool : new ArrayList<>(pool.values())) {
            List<ContainerHandle> toStop;
            synchronized (functionPool) {
                functionPool.generation++;
                toStop = new ArrayList<>(functionPool.queue);
                functionPool.queue.clear();
            }
            stopInParallel(toStop);
        }
    }

    private void evictIdleContainers() {
        if (config == null) return;
        long idleTimeoutMs = config.services().lambda().containerIdleTimeoutSeconds() * 1000L;
        long now = System.currentTimeMillis();

        for (var entry : pool.entrySet()) {
            FunctionIdentity functionIdentity = entry.getKey();
            FunctionPool functionPool = entry.getValue();
            List<ContainerHandle> toEvict = new ArrayList<>();

            synchronized (functionPool) {
                functionPool.queue.removeIf(handle -> {
                    if (handle.getState() == ContainerState.WARM
                            && (now - handle.getLastUsedMs()) >= idleTimeoutMs) {
                        toEvict.add(handle);
                        return true;
                    }
                    return false;
                });
            }

            if (!toEvict.isEmpty()) {
                LOG.infov("Evicting {0} idle container(s) for function: {1}",
                        toEvict.size(), functionIdentity);
                for (ContainerHandle handle : toEvict) {
                    stopQuietly(handle);
                }
            }
        }
    }

    private void stopQuietly(ContainerHandle handle) {
        try {
            containerLauncher.stop(handle);
        } catch (Exception e) {
            LOG.warnv("Error stopping container {0}: {1}", handle.getContainerId(), e.getMessage());
        }
    }

    private record FunctionIdentity(String accountId, String region, String functionName, String version) {
        private static FunctionIdentity from(LambdaExecutionEnvironmentId environmentId) {
            return new FunctionIdentity(
                    environmentId.accountId(),
                    environmentId.region(),
                    environmentId.functionName(),
                    environmentId.version());
        }
    }

    private static final class FunctionPool {
        private final ArrayDeque<ContainerHandle> queue = new ArrayDeque<>();
        private LambdaExecutionEnvironmentId environmentId;
        private long generation;
        private boolean retired;
    }
}
