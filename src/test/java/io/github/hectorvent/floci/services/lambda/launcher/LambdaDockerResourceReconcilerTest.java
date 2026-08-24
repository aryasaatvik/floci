package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.DockerResourceIdentity;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LambdaDockerResourceReconcilerTest {

    @Mock ContainerLifecycleManager lifecycleManager;
    @Mock LambdaFunctionStore functionStore;
    @Mock DockerResourceIdentity resourceIdentity;
    @Mock EmulatorConfig config;
    @Mock EmulatorConfig.ServicesConfig services;
    @Mock EmulatorConfig.LambdaServiceConfig lambda;

    private LambdaDockerResourceReconciler reconciler;

    @BeforeEach
    void setUp() {
        when(resourceIdentity.instanceId()).thenReturn("test-instance");
        reconciler = new LambdaDockerResourceReconciler(
                lifecycleManager, functionStore, resourceIdentity, config);
    }

    @Test
    void removesOnlyExactlyOwnedKnownContainersAndStaleVolumes() {
        stubContainerNamePrefix();
        LambdaFunction activeFunction = function("active", "active-code");
        String activeVolumeName = ContainerLauncher.ownedCodeVolumeName(
                "floci", activeFunction, "test-instance");

        Container execution = container("execution", ownedLabels(LambdaDockerResourceLabels.EXECUTION));
        Container populator = container("populator", ownedLabels(LambdaDockerResourceLabels.CODE_POPULATOR));
        Container unknownKind = container("unknown", ownedLabels("future-kind"));
        Container foreignInstance = container("foreign-instance",
                labels("other-instance", LambdaDockerResourceLabels.EXECUTION));
        Map<String, String> missingManaged = new HashMap<>(ownedLabels(LambdaDockerResourceLabels.EXECUTION));
        missingManaged.remove(LambdaDockerResourceLabels.MANAGED);
        Container incomplete = container("incomplete", Map.copyOf(missingManaged));
        when(lifecycleManager.listContainersByLabels(
                LambdaDockerResourceLabels.owner("test-instance")))
                .thenReturn(List.of(execution, populator, unknownKind, foreignInstance, incomplete));

        InspectVolumeResponse active = volume(activeVolumeName,
                ownedLabels(LambdaDockerResourceLabels.CODE_VOLUME));
        InspectVolumeResponse stale = volume("floci-code-stale-owned",
                ownedLabels(LambdaDockerResourceLabels.CODE_VOLUME));
        InspectVolumeResponse foreign = volume("foreign-volume",
                labels("other-instance", LambdaDockerResourceLabels.CODE_VOLUME));
        when(functionStore.listAllAccounts()).thenReturn(List.of(activeFunction));
        when(lifecycleManager.listVolumesByLabels(
                LambdaDockerResourceLabels.ownedCodeVolume("test-instance")))
                .thenReturn(List.of(active, stale, foreign));

        reconciler.reconcileAtStartup();

        verify(lifecycleManager).stopAndRemove("execution", null);
        verify(lifecycleManager).stopAndRemove("populator", null);
        verify(lifecycleManager, never()).stopAndRemove("unknown", null);
        verify(lifecycleManager, never()).stopAndRemove("foreign-instance", null);
        verify(lifecycleManager, never()).stopAndRemove("incomplete", null);
        verify(lifecycleManager, never()).removeVolume(activeVolumeName);
        verify(lifecycleManager).removeVolume("floci-code-stale-owned");
        verify(lifecycleManager, never()).removeVolume("foreign-volume");
    }

    @Test
    void repeatedReconciliationIsIdempotentAfterResourcesDisappear() {
        stubContainerNamePrefix();
        Container execution = container("execution", ownedLabels(LambdaDockerResourceLabels.EXECUTION));
        InspectVolumeResponse stale = volume("stale", ownedLabels(LambdaDockerResourceLabels.CODE_VOLUME));
        when(lifecycleManager.listContainersByLabels(
                LambdaDockerResourceLabels.owner("test-instance")))
                .thenReturn(List.of(execution), List.of());
        when(functionStore.listAllAccounts()).thenReturn(List.of());
        when(lifecycleManager.listVolumesByLabels(
                LambdaDockerResourceLabels.ownedCodeVolume("test-instance")))
                .thenReturn(List.of(stale), List.of());

        reconciler.reconcileAtStartup();
        reconciler.reconcileAtStartup();

        verify(lifecycleManager, times(1)).stopAndRemove("execution", null);
        verify(lifecycleManager, times(1)).removeVolume("stale");
    }

    @Test
    void resetRemovesExactlyOwnedResourcesAndIsIdempotent() {
        Container execution = container("execution", ownedLabels(LambdaDockerResourceLabels.EXECUTION));
        Container populator = container("populator", ownedLabels(LambdaDockerResourceLabels.CODE_POPULATOR));
        Container unknownKind = container("unknown", ownedLabels("future-kind"));
        Container foreignInstance = container("foreign-instance",
                labels("other-instance", LambdaDockerResourceLabels.EXECUTION));
        Map<String, String> missingManaged = new HashMap<>(ownedLabels(LambdaDockerResourceLabels.EXECUTION));
        missingManaged.remove(LambdaDockerResourceLabels.MANAGED);
        Container incomplete = container("incomplete", Map.copyOf(missingManaged));
        when(lifecycleManager.listContainersByLabels(
                LambdaDockerResourceLabels.owner("test-instance")))
                .thenReturn(List.of(execution, populator, unknownKind, foreignInstance, incomplete), List.of());

        InspectVolumeResponse owned = volume("owned-volume",
                ownedLabels(LambdaDockerResourceLabels.CODE_VOLUME));
        InspectVolumeResponse foreign = volume("foreign-volume",
                labels("other-instance", LambdaDockerResourceLabels.CODE_VOLUME));
        when(lifecycleManager.listVolumesByLabels(
                LambdaDockerResourceLabels.ownedCodeVolume("test-instance")))
                .thenReturn(List.of(owned, foreign), List.of());

        reconciler.clearOwnedResources();
        reconciler.clearOwnedResources();

        verify(lifecycleManager, times(1)).stopAndRemove("execution", null);
        verify(lifecycleManager, times(1)).stopAndRemove("populator", null);
        verify(lifecycleManager, never()).stopAndRemove("unknown", null);
        verify(lifecycleManager, never()).stopAndRemove("foreign-instance", null);
        verify(lifecycleManager, never()).stopAndRemove("incomplete", null);
        verify(lifecycleManager, times(1)).removeVolume("owned-volume");
        verify(lifecycleManager, never()).removeVolume("foreign-volume");
        verify(functionStore, never()).listAllAccounts();
    }

    private void stubContainerNamePrefix() {
        when(config.services()).thenReturn(services);
        when(services.lambda()).thenReturn(lambda);
        when(lambda.containerNamePrefix()).thenReturn(Optional.empty());
    }

    private static Container container(String id, Map<String, String> labels) {
        Container container = mock(Container.class);
        org.mockito.Mockito.lenient().when(container.getId()).thenReturn(id);
        when(container.getLabels()).thenReturn(labels);
        return container;
    }

    private static InspectVolumeResponse volume(String name, Map<String, String> labels) {
        InspectVolumeResponse volume = mock(InspectVolumeResponse.class);
        org.mockito.Mockito.lenient().when(volume.getName()).thenReturn(name);
        when(volume.getLabels()).thenReturn(labels);
        return volume;
    }

    private static Map<String, String> ownedLabels(String kind) {
        return labels("test-instance", kind);
    }

    private static Map<String, String> labels(String instance, String kind) {
        Map<String, String> labels = new HashMap<>(LambdaDockerResourceLabels.owner(instance));
        labels.put(LambdaDockerResourceLabels.KIND, kind);
        return Map.copyOf(labels);
    }

    private static LambdaFunction function(String name, String codeSha) {
        LambdaFunction function = new LambdaFunction();
        function.setFunctionName(name);
        function.setCodeSha256(codeSha);
        function.setCodeLocalPath("/tmp/" + name);
        return function;
    }
}
