package io.github.hectorvent.floci.core.common.docker;

import io.github.hectorvent.floci.config.EmulatorConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DockerResourceIdentityTest {

    @TempDir
    Path tempDir;

    @Test
    void configuredNamespaceIsTheStableIdentity() {
        EmulatorConfig config = config(tempDir, Optional.of(" samva local "));

        assertEquals("namespace:samva-local", new DockerResourceIdentity(config).instanceId());
    }

    @Test
    void persistsGeneratedIdentityAcrossProcessInstances() {
        EmulatorConfig config = config(tempDir, Optional.empty());

        String first = new DockerResourceIdentity(config).instanceId();
        String second = new DockerResourceIdentity(config).instanceId();

        assertEquals(first, second);
        assertFalse(first.startsWith("ephemeral:"));
    }

    private static EmulatorConfig config(Path persistentPath, Optional<String> namespace) {
        EmulatorConfig config = mock(EmulatorConfig.class);
        EmulatorConfig.DockerConfig docker = mock(EmulatorConfig.DockerConfig.class);
        EmulatorConfig.StorageConfig storage = mock(EmulatorConfig.StorageConfig.class);
        when(config.docker()).thenReturn(docker);
        when(docker.resourceNamespace()).thenReturn(namespace);
        when(config.storage()).thenReturn(storage);
        when(storage.persistentPath()).thenReturn(persistentPath.toString());
        return config;
    }
}
