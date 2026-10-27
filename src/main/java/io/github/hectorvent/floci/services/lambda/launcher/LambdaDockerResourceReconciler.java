package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.ContainerTeardowns;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 */
@ApplicationScoped
public class LambdaDockerResourceReconciler {

    private static final Logger LOG = Logger.getLogger(LambdaDockerResourceReconciler.class);
    /** A leftover runtime has no invocation to finish, so it gets the shortest Lambda stop grace. */
    private static final int STOP_TIMEOUT_SECONDS = 1;
    private static final long REMOVAL_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final ContainerLifecycleManager lifecycleManager;
    private final LambdaFunctionStore functionStore;
    private final EmulatorConfig config;

    @Inject
    public LambdaDockerResourceReconciler(ContainerLifecycleManager lifecycleManager,
                                          LambdaFunctionStore functionStore,
                                          EmulatorConfig config) {
        this.lifecycleManager = lifecycleManager;
        this.functionStore = functionStore;
        this.config = config;
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
