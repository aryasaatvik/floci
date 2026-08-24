package io.github.hectorvent.floci.core.common.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.ListVolumesCmd;
import com.github.dockerjava.api.command.ListVolumesResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContainerLifecycleManagerOwnershipTest {

    @Mock DockerClient dockerClient;
    @Mock ImageCacheService imageCacheService;
    @Mock ContainerDetector containerDetector;
    @Mock PortAllocator portAllocator;
    @Mock EmulatorConfig config;

    private ContainerLifecycleManager manager;

    @BeforeEach
    void setUp() {
        manager = new ContainerLifecycleManager(
                dockerClient, imageCacheService, containerDetector, portAllocator, config);
    }

    @Test
    void containerQueryRechecksEveryRequiredLabelLocally() {
        Map<String, String> required = ownerLabels();
        Container owned = container(with(required, "io.floci.kind", "execution"));
        Container foreign = container(with(required, "io.floci.instance", "other-instance"));
        ListContainersCmd command = mock(ListContainersCmd.class, RETURNS_SELF);
        when(dockerClient.listContainersCmd()).thenReturn(command);
        when(command.exec()).thenReturn(List.of(owned, foreign));

        assertEquals(List.of(owned), manager.listContainersByLabels(required));
        verify(command).withShowAll(true);
        verify(command).withLabelFilter(required);
    }

    @Test
    void volumeQueryRechecksEveryRequiredLabelLocally() {
        Map<String, String> required = with(ownerLabels(), "io.floci.kind", "code");
        InspectVolumeResponse owned = volume(required);
        InspectVolumeResponse foreign = volume(with(required, "io.floci.managed", "false"));
        ListVolumesCmd command = mock(ListVolumesCmd.class, RETURNS_SELF);
        ListVolumesResponse response = mock(ListVolumesResponse.class);
        when(dockerClient.listVolumesCmd()).thenReturn(command);
        when(command.exec()).thenReturn(response);
        when(response.getVolumes()).thenReturn(List.of(owned, foreign));

        assertEquals(List.of(owned), manager.listVolumesByLabels(required));
    }

    private static Container container(Map<String, String> labels) {
        Container container = mock(Container.class);
        when(container.getLabels()).thenReturn(labels);
        return container;
    }

    private static InspectVolumeResponse volume(Map<String, String> labels) {
        InspectVolumeResponse volume = mock(InspectVolumeResponse.class);
        when(volume.getLabels()).thenReturn(labels);
        return volume;
    }

    private static Map<String, String> ownerLabels() {
        return Map.of(
                "io.floci.managed", "true",
                "io.floci.instance", "test-instance",
                "io.floci.service", "lambda");
    }

    private static Map<String, String> with(Map<String, String> source, String key, String value) {
        Map<String, String> result = new HashMap<>(source);
        result.put(key, value);
        return Map.copyOf(result);
    }
}
