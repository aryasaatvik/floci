package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.DockerResourceIdentity;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Reclaims orphaned Lambda Docker resources owned by this exact Floci instance. */
@ApplicationScoped
public class LambdaDockerResourceReconciler {

    private static final Logger LOG = Logger.getLogger(LambdaDockerResourceReconciler.class);

    private final ContainerLifecycleManager lifecycleManager;
    private final LambdaFunctionStore functionStore;
    private final DockerResourceIdentity resourceIdentity;
    private final EmulatorConfig config;

    @Inject
    public LambdaDockerResourceReconciler(ContainerLifecycleManager lifecycleManager,
                                          LambdaFunctionStore functionStore,
                                          DockerResourceIdentity resourceIdentity,
                                          EmulatorConfig config) {
        this.lifecycleManager = lifecycleManager;
        this.functionStore = functionStore;
        this.resourceIdentity = resourceIdentity;
        this.config = config;
    }

    /** Runs after persisted Lambda state is loaded and before event-source pollers start. */
    public void reconcileAtStartup() {
        String instanceId = resourceIdentity.instanceId();
        removeOwnedContainers(instanceId);

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

    private void removeOwnedContainers(String instanceId) {
        Map<String, String> requiredLabels = LambdaDockerResourceLabels.owner(instanceId);
        for (Container container : lifecycleManager.listContainersByLabels(requiredLabels)) {
            Map<String, String> labels = container.getLabels();
            if (!isOwned(labels, requiredLabels)) {
                continue;
            }
            String kind = labels.get(LambdaDockerResourceLabels.KIND);
            if (LambdaDockerResourceLabels.EXECUTION.equals(kind)
                    || LambdaDockerResourceLabels.CODE_POPULATOR.equals(kind)) {
                LOG.infov("Removing orphaned Lambda {0} container {1} owned by Floci instance {2}",
                        kind, container.getId(), instanceId);
                lifecycleManager.stopAndRemove(container.getId(), null);
            }
        }
    }

    private static boolean isOwned(Map<String, String> actual, Map<String, String> required) {
        return actual != null && required.entrySet().stream()
                .allMatch(entry -> entry.getValue().equals(actual.get(entry.getKey())));
    }
}
