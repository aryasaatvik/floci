package io.github.hectorvent.floci.core.common.docker;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerCmd;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.services.lambda.launcher.ImageCacheService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContainerLifecycleManagerLivenessTest {

    @Mock DockerClient dockerClient;
    @Mock ImageCacheService imageCacheService;
    @Mock ContainerDetector containerDetector;
    @Mock PortAllocator portAllocator;
    @Mock EmulatorConfig config;

    @Test
    void stoppedContainerIsUnknownRatherThanConfirmedDead() {
        InspectContainerCmd inspect = mock(InspectContainerCmd.class);
        InspectContainerResponse response = mock(InspectContainerResponse.class, RETURNS_DEEP_STUBS);
        when(dockerClient.inspectContainerCmd("stopped")).thenReturn(inspect);
        when(inspect.exec()).thenReturn(response);
        when(response.getState().getRunning()).thenReturn(false);

        ContainerLifecycleManager manager = manager();

        assertEquals(ContainerLifecycleManager.ContainerLiveness.UNKNOWN,
                manager.containerLiveness("stopped"));
        assertFalse(manager.isContainerRunning("stopped"));
    }

    @Test
    void inspectFailureIsUnknownRatherThanConfirmedDead() {
        InspectContainerCmd inspect = mock(InspectContainerCmd.class);
        when(dockerClient.inspectContainerCmd("unreachable")).thenReturn(inspect);
        when(inspect.exec()).thenThrow(new IllegalStateException("daemon unavailable"));

        assertEquals(ContainerLifecycleManager.ContainerLiveness.UNKNOWN,
                manager().containerLiveness("unreachable"));
    }

    @Test
    void missingContainerIsConfirmedDead() {
        InspectContainerCmd inspect = mock(InspectContainerCmd.class);
        when(dockerClient.inspectContainerCmd("missing")).thenReturn(inspect);
        when(inspect.exec()).thenThrow(new NotFoundException("missing"));

        assertEquals(ContainerLifecycleManager.ContainerLiveness.DEAD,
                manager().containerLiveness("missing"));
    }

    @Test
    void runningContainerIsAlive() {
        InspectContainerCmd inspect = mock(InspectContainerCmd.class);
        InspectContainerResponse response = mock(InspectContainerResponse.class, RETURNS_DEEP_STUBS);
        when(dockerClient.inspectContainerCmd("running")).thenReturn(inspect);
        when(inspect.exec()).thenReturn(response);
        when(response.getState().getRunning()).thenReturn(true);

        ContainerLifecycleManager manager = manager();

        assertEquals(ContainerLifecycleManager.ContainerLiveness.ALIVE,
                manager.containerLiveness("running"));
        assertTrue(manager.isContainerRunning("running"));
    }

    private ContainerLifecycleManager manager() {
        return new ContainerLifecycleManager(
                dockerClient, imageCacheService, containerDetector, portAllocator, config);
    }
}
