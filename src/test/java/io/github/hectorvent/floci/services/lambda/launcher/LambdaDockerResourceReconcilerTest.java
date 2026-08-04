package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.DockerResourceIdentity;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LambdaDockerResourceReconcilerTest {

    @Mock ContainerLifecycleManager lifecycleManager;
    @Mock LambdaFunctionStore functionStore;
    @Mock DockerResourceIdentity resourceIdentity;

    private LambdaDockerResourceReconciler reconciler;

    @BeforeEach
    void setUp() {
        when(resourceIdentity.instanceId()).thenReturn("samva-instance");
        reconciler = new LambdaDockerResourceReconciler(
                lifecycleManager, functionStore, resourceIdentity);
    }

    @Test
    void startupRemovesOnlyOwnedExecutionContainersAndStaleCodeVolumes() {
        LambdaFunction activeFunction = function("active", "active-code");
        String activeVolumeName = ContainerLauncher.codeVolumeName(activeFunction, "samva-instance");

        Container execution = container("execution", LambdaDockerResourceLabels.EXECUTION);
        Container populator = container("populator", LambdaDockerResourceLabels.CODE_POPULATOR);
        Container futureKind = container("future", "some-future-kind");
        when(lifecycleManager.listContainersByLabels(LambdaDockerResourceLabels.owner("samva-instance")))
                .thenReturn(List.of(execution, populator, futureKind));

        InspectVolumeResponse active = volume(activeVolumeName);
        InspectVolumeResponse stale = volume("floci-code-stale");
        when(functionStore.listAllAccounts()).thenReturn(List.of(activeFunction));
        when(lifecycleManager.listVolumesByLabels(ownedCodeLabels()))
                .thenReturn(List.of(active, stale));

        reconciler.reconcileAtStartup();

        verify(lifecycleManager).stopAndRemove("execution", null);
        verify(lifecycleManager).stopAndRemove("populator", null);
        verify(lifecycleManager, never()).stopAndRemove("future", null);
        verify(lifecycleManager, never()).removeVolume(activeVolumeName);
        verify(lifecycleManager).removeVolume("floci-code-stale");
    }

    @Test
    void resetRemovesAllOwnedLambdaContainersAndCodeVolumes() {
        Container execution = container("execution", LambdaDockerResourceLabels.EXECUTION);
        InspectVolumeResponse ownedVolume = volume("owned-code");
        when(lifecycleManager.listContainersByLabels(LambdaDockerResourceLabels.owner("samva-instance")))
                .thenReturn(List.of(execution));
        when(lifecycleManager.listVolumesByLabels(ownedCodeLabels()))
                .thenReturn(List.of(ownedVolume));

        reconciler.clear();

        verify(lifecycleManager).stopAndRemove("execution", null);
        verify(lifecycleManager).removeVolume("owned-code");
        verify(functionStore, never()).listAllAccounts();
    }

    private static Container container(String id, String kind) {
        Container container = mock(Container.class);
        org.mockito.Mockito.lenient().when(container.getId()).thenReturn(id);
        when(container.getLabels()).thenReturn(Map.of(LambdaDockerResourceLabels.KIND, kind));
        return container;
    }

    private static InspectVolumeResponse volume(String name) {
        InspectVolumeResponse volume = mock(InspectVolumeResponse.class);
        when(volume.getName()).thenReturn(name);
        return volume;
    }

    private static Map<String, String> ownedCodeLabels() {
        Map<String, String> labels = new java.util.HashMap<>(
                LambdaDockerResourceLabels.owner("samva-instance"));
        labels.put(LambdaDockerResourceLabels.KIND, LambdaDockerResourceLabels.CODE_VOLUME);
        return Map.copyOf(labels);
    }

    private static LambdaFunction function(String name, String codeSha) {
        LambdaFunction function = new LambdaFunction();
        function.setFunctionName(name);
        function.setAccountId("111111111111");
        function.setFunctionArn("arn:aws:lambda:us-east-1:111111111111:function:" + name);
        function.setCodeSha256(codeSha);
        function.setCodeLocalPath("/tmp/" + name);
        return function;
    }
}
