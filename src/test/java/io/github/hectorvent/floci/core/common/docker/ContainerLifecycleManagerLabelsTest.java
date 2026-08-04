package io.github.hectorvent.floci.core.common.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.CreateVolumeCmd;
import com.github.dockerjava.api.command.InspectVolumeCmd;
import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.ListVolumesCmd;
import com.github.dockerjava.api.command.ListVolumesResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContainerLifecycleManagerLabelsTest {

    @Mock DockerClient dockerClient;
    @Mock ImageCacheService imageCacheService;
    @Mock ContainerDetector containerDetector;
    @Mock PortAllocator portAllocator;

    private ContainerLifecycleManager manager;

    @BeforeEach
    void setUp() {
        manager = new ContainerLifecycleManager(
                dockerClient, imageCacheService, containerDetector, portAllocator);
    }

    @Test
    void createAppliesSpecLabelsToDockerContainer() {
        CreateContainerCmd command = mock(CreateContainerCmd.class, RETURNS_SELF);
        CreateContainerResponse response = mock(CreateContainerResponse.class);
        when(dockerClient.createContainerCmd("alpine")).thenReturn(command);
        when(command.exec()).thenReturn(response);
        when(response.getId()).thenReturn("container-id");

        Map<String, String> labels = Map.of("io.floci.instance", "instance-a");
        ContainerSpec spec = minimalSpec(labels);

        manager.create(spec);

        verify(command).withLabels(labels);
    }

    @Test
    void ensureVolumeAddsFlociMarkerWithoutDroppingOwnershipLabels() {
        InspectVolumeCmd inspect = mock(InspectVolumeCmd.class);
        CreateVolumeCmd create = mock(CreateVolumeCmd.class, RETURNS_SELF);
        when(dockerClient.inspectVolumeCmd("code-volume")).thenReturn(inspect);
        when(inspect.exec()).thenThrow(new NotFoundException("missing"));
        when(dockerClient.createVolumeCmd()).thenReturn(create);

        manager.ensureVolume("code-volume", Map.of("io.floci.instance", "instance-a"));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> labels = ArgumentCaptor.forClass(Map.class);
        verify(create).withLabels(labels.capture());
        assertEquals(Map.of(
                "floci", "true",
                "io.floci.instance", "instance-a"), labels.getValue());
    }

    @Test
    void listMethodsDefensivelyRejectResourcesMissingAnExactRequiredLabel() {
        Map<String, String> required = Map.of(
                "io.floci.instance", "instance-a",
                "io.floci.service", "lambda");

        Container matchingContainer = mock(Container.class);
        Container otherContainer = mock(Container.class);
        when(matchingContainer.getLabels()).thenReturn(required);
        when(otherContainer.getLabels()).thenReturn(Map.of("io.floci.instance", "instance-b"));
        ListContainersCmd containers = mock(ListContainersCmd.class, RETURNS_SELF);
        when(dockerClient.listContainersCmd()).thenReturn(containers);
        when(containers.exec()).thenReturn(List.of(matchingContainer, otherContainer));

        InspectVolumeResponse matchingVolume = mock(InspectVolumeResponse.class);
        InspectVolumeResponse otherVolume = mock(InspectVolumeResponse.class);
        when(matchingVolume.getLabels()).thenReturn(required);
        when(otherVolume.getLabels()).thenReturn(Map.of("io.floci.service", "lambda"));
        ListVolumesCmd volumes = mock(ListVolumesCmd.class, RETURNS_SELF);
        ListVolumesResponse response = mock(ListVolumesResponse.class);
        when(dockerClient.listVolumesCmd()).thenReturn(volumes);
        when(volumes.exec()).thenReturn(response);
        when(response.getVolumes()).thenReturn(List.of(matchingVolume, otherVolume));

        assertEquals(List.of(matchingContainer), manager.listContainersByLabels(required));
        assertEquals(List.of(matchingVolume), manager.listVolumesByLabels(required));
    }

    private static ContainerSpec minimalSpec(Map<String, String> labels) {
        return new ContainerSpec(
                "alpine", null, List.of(), null, null, null, Map.of(), List.of(), null,
                List.of(), List.of(), List.of(), null, false, null, List.of(), null, null,
                List.of(), labels);
    }
}
