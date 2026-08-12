package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.DockerResourceIdentity;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Reclaims only orphaned Lambda Docker resources owned by this exact Floci instance. */
@ApplicationScoped
public class LambdaDockerResourceReconciler implements Resettable {

    private static final Logger LOG = Logger.getLogger(LambdaDockerResourceReconciler.class);

    private final ContainerLifecycleManager lifecycleManager;
    private final LambdaFunctionStore functionStore;
    private final DockerResourceIdentity resourceIdentity;

    @Inject
    public LambdaDockerResourceReconciler(ContainerLifecycleManager lifecycleManager,
                                          LambdaFunctionStore functionStore,
                                          DockerResourceIdentity resourceIdentity) {
        this.lifecycleManager = lifecycleManager;
        this.functionStore = functionStore;
        this.resourceIdentity = resourceIdentity;
    }

    /** Runs after persisted Lambda state is loaded and before event-source pollers start. */
    public void reconcileAtStartup() {
        String instanceId = resourceIdentity.instanceId();
        removeOwnedContainers(instanceId);

        Set<String> activeCodeVolumes = functionStore.listAllAccounts().stream()
                .filter(function -> !function.isHotReload() && function.getCodeLocalPath() != null)
                .map(function -> ContainerLauncher.codeVolumeName(function, instanceId))
                .collect(Collectors.toSet());
        for (InspectVolumeResponse volume : ownedCodeVolumes(instanceId)) {
            if (!activeCodeVolumes.contains(volume.getName())) {
                LOG.infov("Removing stale Lambda code volume {0} owned by Floci instance {1}",
                        volume.getName(), instanceId);
                lifecycleManager.removeVolume(volume.getName());
            }
        }
    }

    /** State reset is destructive: remove every Lambda Docker resource owned by this instance. */
    @Override
    public void clear() {
        String instanceId = resourceIdentity.instanceId();
        removeOwnedContainers(instanceId);
        for (InspectVolumeResponse volume : ownedCodeVolumes(instanceId)) {
            lifecycleManager.removeVolume(volume.getName());
        }
    }

    private void removeOwnedContainers(String instanceId) {
        Map<String, String> owner = LambdaDockerResourceLabels.owner(instanceId);
        for (Container container : lifecycleManager.listContainersByLabels(owner)) {
            String kind = container.getLabels().get(LambdaDockerResourceLabels.KIND);
            if (LambdaDockerResourceLabels.EXECUTION.equals(kind)
                    || LambdaDockerResourceLabels.CODE_POPULATOR.equals(kind)) {
                LOG.infov("Removing orphaned Lambda {0} container {1} owned by Floci instance {2}",
                        kind, container.getId(), instanceId);
                lifecycleManager.stopAndRemove(container.getId(), null);
            }
        }
    }

    private List<InspectVolumeResponse> ownedCodeVolumes(String instanceId) {
        Map<String, String> labels = new HashMap<>(LambdaDockerResourceLabels.owner(instanceId));
        labels.put(LambdaDockerResourceLabels.KIND, LambdaDockerResourceLabels.CODE_VOLUME);
        return lifecycleManager.listVolumesByLabels(Map.copyOf(labels));
    }
}
