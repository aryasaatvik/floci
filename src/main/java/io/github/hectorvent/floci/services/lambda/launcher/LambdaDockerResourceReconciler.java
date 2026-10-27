package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ContainerTeardowns;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.services.lambda.LambdaEnvironmentLimiter;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Reclaims the Lambda Docker resources an earlier process of this Floci deployment left behind.
 *
 * <p>A Floci process that exits without draining its warm pool (a crash, a {@code kill -9}, a
 * host restart) leaves its execution containers running and its code volumes in place, and the
 * next process tracks none of them. At startup, after persisted Lambda state is loaded and before
 * event-source pollers run, this removes every execution and code-populator container labelled with
 * this deployment's {@link ContainerStorageHelper#ownerIdentity owner}, then every code volume of
 * that owner which no current function version mounts. Resources of another owner, and unlabelled
 * ones, are never touched.
 *
 * <p>Each leftover execution container counts against the physical environment cap as retiring
 * from before the first poller starts until Docker confirms its removal, which is retried until it
 * succeeds; a restart cannot admit new environments past containers that may still run.
 */
@ApplicationScoped
public class LambdaDockerResourceReconciler {

    private static final Logger LOG = Logger.getLogger(LambdaDockerResourceReconciler.class);
    /** A leftover runtime has no invocation to finish, so it gets the shortest Lambda stop grace. */
    private static final int STOP_TIMEOUT_SECONDS = 1;
    private static final long REMOVAL_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long RETRY_DELAY_SECONDS = 1;

    private final ContainerLifecycleManager lifecycleManager;
    private final LambdaFunctionStore functionStore;
    private final EmulatorConfig config;
    private final LambdaEnvironmentLimiter environmentLimiter;
    private final ScheduledExecutorService retries;
    /** The slot each leftover execution container holds until its removal is confirmed. */
    private final Map<String, LambdaEnvironmentLimiter.Slot> recovered = new ConcurrentHashMap<>();

    @Inject
    public LambdaDockerResourceReconciler(ContainerLifecycleManager lifecycleManager,
                                          LambdaFunctionStore functionStore,
                                          EmulatorConfig config,
                                          LambdaEnvironmentLimiter environmentLimiter) {
        this(lifecycleManager, functionStore, config, environmentLimiter,
                Executors.newSingleThreadScheduledExecutor(
                        Thread.ofPlatform().daemon().name("lambda-leftover-removal").factory()));
    }

    LambdaDockerResourceReconciler(ContainerLifecycleManager lifecycleManager,
                                   LambdaFunctionStore functionStore,
                                   EmulatorConfig config,
                                   LambdaEnvironmentLimiter environmentLimiter,
                                   ScheduledExecutorService retries) {
        this.lifecycleManager = lifecycleManager;
        this.functionStore = functionStore;
        this.config = config;
        this.environmentLimiter = environmentLimiter;
        this.retries = retries;
    }

    @PreDestroy
    void shutdown() {
        retries.shutdownNow();
    }

    public void reconcileAtStartup() {
        Map<String, String> owner = LambdaDockerResourceLabels.owner(config);
        List<Container> leftovers;
        try {
            leftovers = lifecycleManager.listContainersByLabels(owner).stream()
                    .filter(container -> isLeftover(container.getLabels()))
                    .toList();
        } catch (RuntimeException e) {
            LOG.warnv("Could not list the Lambda containers an earlier Floci process left: {0}", e.getMessage());
            return;
        }
        for (Container container : leftovers) {
            if (LambdaDockerResourceLabels.EXECUTION.equals(
                    container.getLabels().get(ContainerStorageHelper.COMPONENT_LABEL))) {
                recovered.computeIfAbsent(container.getId(), ignored -> environmentLimiter.reserveRecovered());
            }
        }
        ContainerTeardowns.stopInParallel(leftovers, this::removeLeftover, LOG,
                System.nanoTime() + REMOVAL_DEADLINE_NANOS);
        removeUnusedCodeVolumes(owner);
    }

    private static boolean isLeftover(Map<String, String> labels) {
        String component = labels == null ? null : labels.get(ContainerStorageHelper.COMPONENT_LABEL);
        return LambdaDockerResourceLabels.EXECUTION.equals(component)
                || LambdaDockerResourceLabels.CODE_POPULATOR.equals(component);
    }

    private void removeLeftover(Container container) {
        Map<String, String> labels = container.getLabels();
        try {
            lifecycleManager.stopAndRemoveStrict(container.getId(), null, STOP_TIMEOUT_SECONDS);
            LOG.infov("Removed Lambda {0} container {1} of {2}, left by an earlier Floci process",
                    labels.get(ContainerStorageHelper.COMPONENT_LABEL), container.getId(),
                    labels.get(LambdaDockerResourceLabels.FUNCTION_LABEL));
        } catch (RuntimeException e) {
            LOG.warnv("Could not remove Lambda container {0} left by an earlier Floci process: {1}",
                    container.getId(), e.getMessage());
            if (recovered.containsKey(container.getId())) {
                scheduleRetry(container);
            }
            return;
        }
        LambdaEnvironmentLimiter.Slot slot = recovered.remove(container.getId());
        if (slot != null) {
            slot.close();
        }
    }

    private void scheduleRetry(Container container) {
        try {
            retries.schedule(() -> removeLeftover(container), RETRY_DELAY_SECONDS, TimeUnit.SECONDS);
        } catch (RejectedExecutionException e) {
            // Shutting down: the slot stays held rather than freeing room the container may still use.
            LOG.debugv("Not retrying removal of Lambda container {0}: Floci is stopping", container.getId());
        }
    }

    private void removeUnusedCodeVolumes(Map<String, String> owner) {
        Map<String, String> labels = new HashMap<>(owner);
        labels.put(ContainerStorageHelper.COMPONENT_LABEL, LambdaDockerResourceLabels.CODE_VOLUME);
        List<InspectVolumeResponse> volumes;
        try {
            volumes = lifecycleManager.listVolumesByLabels(labels);
        } catch (RuntimeException e) {
            LOG.warnv("Could not list Lambda code volumes: {0}", e.getMessage());
            return;
        }
        Set<String> mounted = functionStore.listAll().stream()
                .filter(fn -> !fn.isHotReload() && fn.getCodeLocalPath() != null)
                .map(fn -> ContainerLauncher.codeVolumeName(config, fn))
                .collect(Collectors.toSet());
        for (InspectVolumeResponse volume : volumes) {
            if (!mounted.contains(volume.getName()) && lifecycleManager.removeVolume(volume.getName())) {
                LOG.infov("Removed Lambda code volume {0}: no function version mounts it", volume.getName());
            }
        }
    }
}
