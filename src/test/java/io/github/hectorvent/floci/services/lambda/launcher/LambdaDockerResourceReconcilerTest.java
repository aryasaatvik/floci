package io.github.hectorvent.floci.services.lambda.launcher;

import com.github.dockerjava.api.command.InspectVolumeResponse;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.DockerResourceIdentity;
import io.github.hectorvent.floci.services.lambda.LambdaEnvironmentLimiter;
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
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyString;

@ExtendWith(MockitoExtension.class)
class LambdaDockerResourceReconcilerTest {

    @Mock ContainerLifecycleManager lifecycleManager;
    @Mock LambdaFunctionStore functionStore;
    @Mock DockerResourceIdentity resourceIdentity;
    @Mock EmulatorConfig config;
    @Mock EmulatorConfig.ServicesConfig services;
    @Mock EmulatorConfig.LambdaServiceConfig lambda;
    @Mock LambdaEnvironmentLimiter environmentLimiter;
    @Mock LambdaEnvironmentLimiter.Permit recoveredPermit;

    private LambdaDockerResourceReconciler reconciler;

    @BeforeEach
    void setUp() {
        when(resourceIdentity.instanceId()).thenReturn("test-instance");
        org.mockito.Mockito.lenient().when(environmentLimiter.reserveRecovered(anyString()))
                .thenReturn(recoveredPermit);
        org.mockito.Mockito.lenient().when(lifecycleManager.containerLiveness(
                        org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(ContainerLifecycleManager.ContainerLiveness.DEAD);
        reconciler = new LambdaDockerResourceReconciler(
                lifecycleManager, functionStore, resourceIdentity, config, environmentLimiter);
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
    void recoveredExecutionConsumesPhysicalCapacityBeforeTeardownCompletes() throws Exception {
        when(config.services()).thenReturn(services);
        when(services.lambda()).thenReturn(lambda);
        when(lambda.maxPhysicalEnvironments()).thenReturn(OptionalInt.of(1));
        when(lambda.physicalEnvironmentWaitTimeoutSeconds()).thenReturn(1);
        when(lambda.containerNamePrefix()).thenReturn(Optional.empty());

        LambdaEnvironmentLimiter realLimiter = new LambdaEnvironmentLimiter(config);
        LambdaDockerResourceReconciler realReconciler = new LambdaDockerResourceReconciler(
                lifecycleManager, functionStore, resourceIdentity, config, realLimiter);
        Container execution = container("execution", ownedLabels(LambdaDockerResourceLabels.EXECUTION));
        when(lifecycleManager.listContainersByLabels(
                LambdaDockerResourceLabels.owner("test-instance")))
                .thenReturn(List.of(execution));
        when(functionStore.listAllAccounts()).thenReturn(List.of());
        when(lifecycleManager.listVolumesByLabels(
                LambdaDockerResourceLabels.ownedCodeVolume("test-instance")))
                .thenReturn(List.of());

        CountDownLatch stopStarted = new CountDownLatch(1);
        CountDownLatch allowStop = new CountDownLatch(1);
        doAnswer(invocation -> {
            stopStarted.countDown();
            assertTrue(allowStop.await(2, TimeUnit.SECONDS), "reconciliation teardown did not proceed");
            return null;
        }).when(lifecycleManager).stopAndRemove("execution", null);
        when(lifecycleManager.containerLiveness("execution"))
                .thenReturn(ContainerLifecycleManager.ContainerLiveness.DEAD);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> reconciliation = executor.submit(realReconciler::reconcileAtStartup);
            assertTrue(stopStarted.await(2, TimeUnit.SECONDS), "reconciliation did not reserve execution");
            assertEquals(1, realLimiter.status().total());
            assertEquals(1, realLimiter.status().retiring());
            assertEquals(0, realLimiter.status().available());

            Future<LambdaEnvironmentLimiter.Permit> waiter = executor.submit(
                    () -> realLimiter.acquireInterruptibly("new-environment"));
            awaitQueued(realLimiter, 1);
            assertFalse(waiter.isDone(), "new admission bypassed recovered physical ownership");

            allowStop.countDown();
            reconciliation.get(2, TimeUnit.SECONDS);
            LambdaEnvironmentLimiter.Permit permit = waiter.get(2, TimeUnit.SECONDS);
            permit.close();
            assertEquals(0, realLimiter.status().total());
        } finally {
            allowStop.countDown();
            executor.shutdownNow();
            realReconciler.shutdown();
            realLimiter.close();
        }
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

    private static void awaitQueued(LambdaEnvironmentLimiter limiter, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (limiter.queuedCount() < expected && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(expected, limiter.queuedCount(), "expected waiter was not queued");
    }
}
