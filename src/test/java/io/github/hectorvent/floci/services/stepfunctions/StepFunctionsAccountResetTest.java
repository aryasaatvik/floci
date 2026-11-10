package io.github.hectorvent.floci.services.stepfunctions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.stepfunctions.model.Execution;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class StepFunctionsAccountResetTest {
    @TempDir Path directory;

    @Test
    void runningExecutionRefusesResetWhileOtherAccountDoesNotBlock() {
        String target = "111111111111";
        String other = "222222222222";
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.defaultAccountId()).thenReturn(target);
        when(config.storage().persistentPath()).thenReturn(directory.toString());
        ServiceConfigAccess access = mock(ServiceConfigAccess.class);
        when(access.storageMode(anyString())).thenReturn("memory");
        StorageFactory factory = new StorageFactory(config, access);
        StepFunctionsService service = new StepFunctionsService(factory, new RegionResolver("us-east-1", target),
                null, new ObjectMapper(), mock(SfnMockLoader.class), mock(StepFunctionsEventPublisher.class));
        AccountAwareStorageBackend<Execution> executions = factory.create("stepfunctions", "sfn-executions.json",
                new TypeReference<Map<String, Execution>>() {});
        Execution waiting = new Execution();
        waiting.setStatus("RUNNING");
        executions.putForAccount(target, "target-wait", waiting);
        Execution foreign = new Execution();
        foreign.setStatus("RUNNING");
        executions.putForAccount(other, "other-wait", foreign);
        assertEquals(409, assertThrows(AwsException.class, () -> service.checkAccountReset(target)).getHttpStatus());
        waiting.setStatus("SUCCEEDED");
        assertDoesNotThrow(() -> service.checkAccountReset(target));
        service.clearAccount(target);
        assertTrue(executions.getForAccount(target, "target-wait").isEmpty());
        assertEquals("RUNNING", executions.getForAccount(other, "other-wait").orElseThrow().getStatus());
        factory.shutdownAll();
    }
}
