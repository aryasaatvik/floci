package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.DockerResourceIdentity;
import io.github.hectorvent.floci.services.lambda.LambdaEnvironmentLimiter;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Reclaims orphaned Lambda Docker resources owned by this exact Floci instance. */
@ApplicationScoped
public class LambdaDockerResourceReconciler {

    private static final Logger LOG = Logger.getLogger(LambdaDockerResourceReconciler.class);
    private static final long RETIREMENT_RETRY_DELAY_MS = 250;

    private final ContainerLifecycleManager lifecycleManager;
    private final LambdaFunctionStore functionStore;
    private final DockerResourceIdentity resourceIdentity;
    private final EmulatorConfig config;
    private final LambdaEnvironmentLimiter environmentLimiter;
    /** Physical permits retained until Docker confirms a recovered container is gone. */
    private final ConcurrentHashMap<String, LambdaEnvironmentLimiter.Permit> pendingRetirements =
            new ConcurrentHashMap<>();
    /** Prevents a reset or repeated startup pass from overlapping teardown for one container. */
    private final Set<String> retirementsInFlight = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService retirementExecutor = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread thread = new Thread(r, "floci-lambda-reconciler");
                thread.setDaemon(true);
                return thread;
            });

    @Inject
    public LambdaDockerResourceReconciler(ContainerLifecycleManager lifecycleManager,
                                          LambdaFunctionStore functionStore,
                                          DockerResourceIdentity resourceIdentity,
                                          EmulatorConfig config,
                                          LambdaEnvironmentLimiter environmentLimiter) {
        this.lifecycleManager = lifecycleManager;
        this.functionStore = functionStore;
        this.resourceIdentity = resourceIdentity;
        this.config = config;
        this.environmentLimiter = environmentLimiter;
    }

    /** Runs after persisted Lambda state is loaded and before event-source pollers start. */
    public void reconcileAtStartup() {
        String instanceId = resourceIdentity.instanceId();
        reconcileOwnedContainers(instanceId);

        String namePrefix = ContainerLauncher.resolveContainerNamePrefix(config);
        Set<String> activeCodeVolumes = functionStore.listAllAccounts().stream()
                .filter(function -> !function.isHotReload() && function.getCodeLocalPath() != null)
                .map(function -> ContainerLauncher.ownedCodeVolumeName(
                        namePrefix, function, instanceId))
                .collect(Collectors.toSet());
        Map<String, String> requiredLabels = LambdaDockerResourceLabels.ownedCodeVolume(instanceId);
        for (InspectVolumeResponse volume : lifecycleManager.listVolumesByLabels(requiredLabels)) {
            if (isOwned(volume.getLabels(), requiredLabels)
                    && !activeCodeVolumes.contains(volume.getName())) {
                LOG.infov("Removing stale Lambda code volume {0} owned by Floci instance {1}",
                        volume.getName(), instanceId);
                lifecycleManager.removeVolume(volume.getName());
            }
        }
    }

    /** Removes all Lambda Docker resources owned by this exact Floci instance during reset. */
    public void clearOwnedResources() {
        String instanceId = resourceIdentity.instanceId();
        removeOwnedContainers(instanceId);

        Map<String, String> requiredLabels = LambdaDockerResourceLabels.ownedCodeVolume(instanceId);
        for (InspectVolumeResponse volume : lifecycleManager.listVolumesByLabels(requiredLabels)) {
            if (isOwned(volume.getLabels(), requiredLabels)) {
                LOG.infov("Removing Lambda code volume {0} owned by Floci instance {1} during reset",
                        volume.getName(), instanceId);
                lifecycleManager.removeVolume(volume.getName());
            }
        }
    }

    @PreDestroy
    void shutdown() {
        retirementExecutor.shutdownNow();
    }

    /**
     * Reconciles execution containers before pollers or request traffic can create new ones.
     * Every discovered execution container first reserves a physical permit in the F1 limiter;
     * the permit remains in {@code retiring} until Docker confirms the container is gone. This
     * preserves the global active-plus-warm cap across a process restart and fails closed when
     * the daemon cannot immediately complete cleanup.
     */
    private void reconcileOwnedContainers(String instanceId) {
        Map<String, String> requiredLabels = LambdaDockerResourceLabels.owner(instanceId);
        for (Container container : lifecycleManager.listContainersByLabels(requiredLabels)) {
            Map<String, String> labels = container.getLabels();
            if (!isOwned(labels, requiredLabels)) {
                continue;
            }
            String kind = labels.get(LambdaDockerResourceLabels.KIND);
            String containerId = container.getId();
            if (LambdaDockerResourceLabels.EXECUTION.equals(kind)) {
                if (containerId == null || containerId.isBlank()
                        || pendingRetirements.containsKey(containerId)) {
                    continue;
                }
                LambdaEnvironmentLimiter.Permit permit = environmentLimiter.reserveRecovered(
                        environmentKey(labels));
                LambdaEnvironmentLimiter.Permit previous = pendingRetirements.putIfAbsent(
                        containerId, permit);
                if (previous != null) {
                    // A concurrent reconciliation won the ownership race.
                    permit.close();
                    continue;
                }
                retireRecoveredContainer(containerId, permit);
            } else if (LambdaDockerResourceLabels.CODE_POPULATOR.equals(kind)) {
                removeContainer(containerId, kind, instanceId);
            }
        }
    }

    private void removeOwnedContainers(String instanceId) {
        Map<String, String> requiredLabels = LambdaDockerResourceLabels.owner(instanceId);
        for (Container container : lifecycleManager.listContainersByLabels(requiredLabels)) {
            Map<String, String> labels = container.getLabels();
            if (!isOwned(labels, requiredLabels)) {
                continue;
            }
            String kind = labels.get(LambdaDockerResourceLabels.KIND);
            String containerId = container.getId();
            if (containerId == null || containerId.isBlank()) {
                LOG.warnv("Skipping owned Lambda {0} container without an id during cleanup", kind);
                continue;
            }
            if (LambdaDockerResourceLabels.EXECUTION.equals(kind)
                    || LambdaDockerResourceLabels.CODE_POPULATOR.equals(kind)) {
                LambdaEnvironmentLimiter.Permit permit = pendingRetirements.get(containerId);
                if (permit != null) {
                    retireRecoveredContainer(containerId, permit);
                } else {
                    removeContainer(containerId, kind, instanceId);
                }
            }
        }
    }

    private void removeContainer(String containerId, String kind, String instanceId) {
        LOG.infov("Removing orphaned Lambda {0} container {1} owned by Floci instance {2}",
                kind, containerId, instanceId);
        lifecycleManager.stopAndRemove(containerId, null);
    }

    private void retireRecoveredContainer(String containerId,
                                           LambdaEnvironmentLimiter.Permit permit) {
        if (pendingRetirements.get(containerId) != permit
                || !retirementsInFlight.add(containerId)) {
            return;
        }
        try {
            try {
                LOG.infov("Reconciling recovered Lambda execution container {0}", containerId);
                lifecycleManager.stopAndRemove(containerId, null);
            } catch (RuntimeException e) {
                LOG.warnv("Could not remove recovered Lambda container {0}: {1}",
                        containerId, e.getMessage());
            }
            ContainerLifecycleManager.ContainerLiveness liveness;
            try {
                liveness = lifecycleManager.containerLiveness(containerId);
            } catch (RuntimeException e) {
                LOG.warnv("Could not probe recovered Lambda container {0}: {1}",
                        containerId, e.getMessage());
                liveness = ContainerLifecycleManager.ContainerLiveness.UNKNOWN;
            }
            if (liveness == ContainerLifecycleManager.ContainerLiveness.DEAD) {
                if (pendingRetirements.remove(containerId, permit)) {
                    permit.close();
                }
            } else {
                scheduleRetirementRetry(containerId, permit);
            }
        } finally {
            retirementsInFlight.remove(containerId);
        }
    }

    private void scheduleRetirementRetry(String containerId,
                                          LambdaEnvironmentLimiter.Permit permit) {
        if (pendingRetirements.get(containerId) != permit) {
            return;
        }
        try {
            retirementExecutor.schedule(
                    () -> retireRecoveredContainer(containerId, permit),
                    RETIREMENT_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // Keep the permit in pendingRetirements. Releasing it without a confirmed Docker
            // removal would allow a surviving process-owned container to exceed the cap.
            LOG.warnv("Could not schedule recovered Lambda retirement for {0}; retaining permit",
                    containerId);
        }
    }

    private static String environmentKey(Map<String, String> labels) {
        String explicit = labels == null ? null : labels.get(LambdaDockerResourceLabels.ENVIRONMENT);
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        // Containers created before the environment label was introduced are still safely
        // counted against the global cap. The fallback is intentionally stable for status
        // readback, even when the persisted Lambda function is no longer available.
        String account = valueOr(labels, "io.floci.account", "000000000000");
        String function = valueOr(labels, LambdaDockerResourceLabels.FUNCTION, "unknown");
        String code = valueOr(labels, LambdaDockerResourceLabels.CODE, "unknown");
        return account + ":" + function + ":" + code;
    }

    private static String valueOr(Map<String, String> labels, String key, String fallback) {
        String value = labels == null ? null : labels.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static boolean isOwned(Map<String, String> actual, Map<String, String> required) {
        return actual != null && required.entrySet().stream()
                .allMatch(entry -> entry.getValue().equals(actual.get(entry.getKey())));
    }
}
