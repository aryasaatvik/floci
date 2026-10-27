package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.services.lambda.LambdaFunctionStore;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LambdaDockerResourceReconcilerTest {

    private static final Map<String, String> OWNER = Map.of("io.floci.service", "lambda", "io.floci.owner", "4566");

    private ContainerLifecycleManager lifecycleManager;
    private LambdaFunctionStore functionStore;
    private EmulatorConfig config;
    private LambdaDockerResourceReconciler reconciler;

    @BeforeEach
    void setUp() {
        lifecycleManager = mock(ContainerLifecycleManager.class);
        functionStore = mock(LambdaFunctionStore.class);
        config = mock(EmulatorConfig.class);
        EmulatorConfig.DockerConfig docker = mock(EmulatorConfig.DockerConfig.class);
        when(docker.resourceNamespace()).thenReturn(Optional.empty());
        when(config.docker()).thenReturn(docker);
        when(config.port()).thenReturn(4566);
        EmulatorConfig.ServicesConfig services = mock(EmulatorConfig.ServicesConfig.class);
        EmulatorConfig.LambdaServiceConfig lambda = mock(EmulatorConfig.LambdaServiceConfig.class);
        when(config.services()).thenReturn(services);
        when(services.lambda()).thenReturn(lambda);
        when(lambda.containerNamePrefix()).thenReturn(Optional.empty());
        when(functionStore.listAll()).thenReturn(List.of());
        when(lifecycleManager.listVolumesByLabels(any())).thenReturn(List.of());
        reconciler = new LambdaDockerResourceReconciler(lifecycleManager, functionStore, config);
    }

    @Test
    void removesThisOwnersLeftoverExecutionAndPopulatorContainers() {
        List<Container> containers = List.of(
                container("exec-1", "execution"),
                container("populator-1", "code-populator"),
                container("unknown-1", "something-else"));
        when(lifecycleManager.listContainersByLabels(OWNER)).thenReturn(containers);

        reconciler.reconcileAtStartup();

        verify(lifecycleManager).stopAndRemoveStrict(eq("exec-1"), any(), anyInt());
        verify(lifecycleManager).stopAndRemoveStrict(eq("populator-1"), any(), anyInt());
        verify(lifecycleManager, never()).stopAndRemoveStrict(eq("unknown-1"), any(), anyInt());
    }

    @Test
    void aContainerThatCannotBeRemovedDoesNotStopTheRest() {
        List<Container> containers = List.of(container("stuck", "execution"), container("exec-2", "execution"));
        when(lifecycleManager.listContainersByLabels(OWNER)).thenReturn(containers);
        doThrow(new RuntimeException("daemon busy"))
                .when(lifecycleManager).stopAndRemoveStrict(eq("stuck"), any(), anyInt());

        reconciler.reconcileAtStartup();

        verify(lifecycleManager).stopAndRemoveStrict(eq("exec-2"), any(), anyInt());
    }

    @Test
    void removesOnlyCodeVolumesNoFunctionVersionMounts() {
        LambdaFunction current = new LambdaFunction();
        current.setFunctionName("orders");
        current.setCodeSha256("current-sha");
        current.setCodeLocalPath("/app/data/lambda/orders");
        when(functionStore.listAll()).thenReturn(List.of(current));
        String mounted = ContainerLauncher.codeVolumeName(config, current);
        when(lifecycleManager.listContainersByLabels(OWNER)).thenReturn(List.of());
        List<InspectVolumeResponse> volumes = List.of(volume(mounted), volume("floci-aws-code-orders-old"));
        when(lifecycleManager.listVolumesByLabels(Map.of("io.floci.service", "lambda", "io.floci.owner", "4566",
                "io.floci.component", "code-volume"))).thenReturn(volumes);

        reconciler.reconcileAtStartup();

        verify(lifecycleManager).removeVolume("floci-aws-code-orders-old");
        verify(lifecycleManager, never()).removeVolume(mounted);
    }

    @Test
    void anUnreachableDaemonLeavesStartupToContinue() {
        when(lifecycleManager.listContainersByLabels(any())).thenThrow(new RuntimeException("no docker"));

        reconciler.reconcileAtStartup();

        verify(lifecycleManager, never()).stopAndRemoveStrict(anyString(), any(), anyInt());
        verify(lifecycleManager, never()).removeVolume(anyString());
    }

    private static Container container(String id, String component) {
        Container container = mock(Container.class);
        when(container.getId()).thenReturn(id);
        when(container.getLabels()).thenReturn(Map.of("io.floci.service", "lambda", "io.floci.owner", "4566",
                "io.floci.component", component, "io.floci.lambda.function", "orders"));
        return container;
    }

    private static InspectVolumeResponse volume(String name) {
        InspectVolumeResponse volume = mock(InspectVolumeResponse.class);
        when(volume.getName()).thenReturn(name);
        return volume;
    }
}
