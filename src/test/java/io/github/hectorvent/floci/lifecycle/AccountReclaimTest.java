package io.github.hectorvent.floci.lifecycle;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.Container;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AccountActivity;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.ContainerTeardown;
import io.github.hectorvent.floci.core.common.FileTrees;
import io.github.hectorvent.floci.core.common.Resettable;
import io.github.hectorvent.floci.core.common.ServiceConfigAccess;
import io.github.hectorvent.floci.core.common.docker.ContainerLifecycleManager;
import io.github.hectorvent.floci.core.common.docker.ContainerStorageHelper;
import io.github.hectorvent.floci.core.storage.AccountAwareStorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.quarkus.arc.InjectableContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AccountReclaimTest {

    private static final String TARGET = "111111111111";
    private static final String FOREIGN = "222222222222";
    private static final Map<String, String> TARGET_CONTAINERS = Map.of(
            ContainerStorageHelper.ACCOUNT_LABEL, TARGET,
            "floci", "true");

    @TempDir Path directory;

    private final AccountActivity activity = new AccountActivity();
    private final Resettable guard = mock(Resettable.class);
    private final AtomicReference<List<Resettable>> live = new AtomicReference<>(List.of(guard));
    private final ContainerLifecycleManager containers = mock(ContainerLifecycleManager.class);
    private final StorageFactory storage = mock(StorageFactory.class);
    @SuppressWarnings("unchecked")
    private final Instance<Resettable> services = mock(Instance.class);
    @SuppressWarnings("unchecked")
    private final Instance<ContainerTeardown> teardowns = mock(Instance.class);
    private EmulatorConfig config;
    private EmulatorInfoController controller;

    @BeforeEach
    void setUp() {
        config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.storage().persistentPath()).thenReturn(directory.toString());
        when(config.storage().services().s3().mode()).thenReturn(Optional.of("persistent"));
        when(config.services().lambda().codePath()).thenReturn(directory.resolve("lambda-code").toString());
        when(config.docker().resourceNamespace()).thenReturn(Optional.of("test"));
        when(config.port()).thenReturn(4566);
        when(config.defaultAccountId()).thenReturn("000000000000");
        controller = controllerUsing(storage);
    }

    private EmulatorInfoController controllerUsing(StorageFactory factory) {
        AccountReclaim reclaim = new AccountReclaim(activity, config, live::get, containers, factory);
        return new EmulatorInfoController(null, null, factory, services, teardowns,
                null, config, null, null, reclaim);
    }

    @Test
    void previewSizesScopedFilesByMetadataAndKeepsUnopenedStoresUntouched() throws Exception {
        Path payload = write("s3/.accounts/" + TARGET + "/bucket/data.s3data", "target");
        Path code = write("lambda-code/" + TARGET + "/@regions/us-east-1/function/script", "lambda code");
        Path foreign = write("s3/.accounts/" + FOREIGN + "/bucket/data.s3data", "do not count foreign");
        Path unopenedStore = write("unopened.json", "not valid JSON and must never be loaded");
        Path legacy = write("s3/legacy-bucket/data.s3data", "do not count legacy");
        Path link = Files.createSymbolicLink(payload.resolveSibling("outside"), foreign);
        FileTime marker = FileTime.from(Instant.EPOCH);
        Files.setLastModifiedTime(unopenedStore, marker);
        List<Path> before = entries();

        Response response = controller.previewAccountReclaim(TARGET);

        assertEquals(200, response.getStatus());
        assertEquals(new AccountReclaim.Preview(TARGET, true, null, Files.size(payload), Files.size(code)),
                response.getEntity());
        assertEquals(
                "{\"accountId\":\"111111111111\",\"eligible\":true,\"blocker\":null,\"s3Bytes\":6,\"lambdaCodeBytes\":11}",
                new ObjectMapper().writeValueAsString(response.getEntity()));
        assertEquals(before, entries());
        assertEquals(marker, Files.getLastModifiedTime(unopenedStore));
        assertTrue(Files.isSymbolicLink(link));
        assertEquals("do not count legacy", Files.readString(legacy));
        verify(guard).checkAccountReset(TARGET);
        verify(containers).listContainersByLabels(TARGET_CONTAINERS);
        verifyNoMoreInteractions(guard, containers);
        verifyNoInteractions(storage, services, teardowns);
    }

    @Test
    void previewRejectsASymlinkAtAnAccountPathBoundary() throws Exception {
        Path foreign = write("external/data.s3data", "private");
        Path account = directory.resolve("s3/.accounts/" + TARGET);
        Files.createDirectories(account.getParent());
        Files.createSymbolicLink(account, foreign.getParent());

        assertThrows(IOException.class, () -> controller.previewAccountReclaim(TARGET));
        assertThrows(IOException.class, () -> controller.reclaimAccount(TARGET, List.of(TARGET)));
        assertEquals("private", Files.readString(foreign));
        assertTrue(Files.isSymbolicLink(account));
        verifyNoInteractions(storage, services, teardowns);
    }

    @Test
    void malformedOrAmbiguousConfirmationLeavesStateUntouched() throws Exception {
        assertEquals(400, controller.previewAccountReclaim("１２３４５６７８９０１２").getStatus());
        assertEquals(400, controller.reclaimAccount("../22222222", List.of(TARGET)).getStatus());
        assertEquals(400, controller.reclaimAccount(TARGET, List.of()).getStatus());
        assertEquals(400, controller.reclaimAccount(TARGET, List.of(FOREIGN)).getStatus());
        assertEquals(400, controller.reclaimAccount(TARGET, List.of(TARGET, TARGET)).getStatus());
        assertEquals(400, controller.reclaimAccount(TARGET, List.of(TARGET + " ")).getStatus());
        verifyNoInteractions(guard, containers, storage, services, teardowns);
    }

    @Test
    void runtimeRevivedAfterPreviewPreventsAnyClear() throws Exception {
        assertTrue(((AccountReclaim.Preview) controller.previewAccountReclaim(TARGET).getEntity()).eligible());
        Resettable lateGuard = mock(Resettable.class);
        doThrow(new AwsException("ConflictException", "private details", 409)).when(lateGuard)
                .checkAccountReset(TARGET);
        live.set(List.of(guard, lateGuard));

        Response response = controller.reclaimAccount(TARGET, List.of(TARGET));

        assertEquals(409, response.getStatus());
        assertEquals(Map.of("error", "account-busy", "accountId", TARGET), response.getEntity());
        verify(guard, never()).clearAccount(TARGET);
        verify(lateGuard, never()).clearAccount(TARGET);
        verifyNoInteractions(storage, teardowns);
        assertFalse(activity.isBusy(TARGET));
    }

    @Test
    void accountBecomingBusyWhilePreparingStoragePreventsAllClears() throws Exception {
        Resettable lateGuard = mock(Resettable.class);
        doThrow(new AwsException("ConflictException", "busy", 409)).when(lateGuard).checkAccountReset(TARGET);
        StorageFactory.PreparedAccountReclaim prepared = mock(StorageFactory.PreparedAccountReclaim.class);
        when(storage.prepareAccountReclaim(TARGET)).thenAnswer(ignored -> {
            live.set(List.of(guard, lateGuard));
            return prepared;
        });

        assertEquals(409, controller.reclaimAccount(TARGET, List.of(TARGET)).getStatus());

        verify(guard, never()).clearAccount(TARGET);
        verify(lateGuard, never()).clearAccount(TARGET);
        verifyNoInteractions(prepared, services, teardowns);
    }

    @Test
    void containerDependencyBlocksPreviewAndReclaimWithoutStoppingIt() throws Exception {
        Container dependency = mock(Container.class);
        when(dependency.getLabels()).thenReturn(Map.of("floci_namespace", "test"));
        when(containers.listContainersByLabels(TARGET_CONTAINERS)).thenReturn(List.of(dependency));
        assertEquals(new AccountReclaim.Preview(TARGET, false, "account-busy", 0, 0),
                controller.previewAccountReclaim(TARGET).getEntity());
        assertEquals(409, controller.reclaimAccount(TARGET, List.of(TARGET)).getStatus());
        verify(containers, times(2)).listContainersByLabels(TARGET_CONTAINERS);
        verifyNoMoreInteractions(containers);
        verifyNoInteractions(storage, services, teardowns);
    }

    @Test
    void aMatchingAccountInAnotherDockerNamespaceDoesNotBlock() throws Exception {
        Container foreign = mock(Container.class);
        when(foreign.getLabels()).thenReturn(Map.of("floci_namespace", "different"));
        when(containers.listContainersByLabels(TARGET_CONTAINERS)).thenReturn(List.of(foreign));
        AccountReclaim.Preview preview = (AccountReclaim.Preview) controller.previewAccountReclaim(TARGET).getEntity();
        assertTrue(preview.eligible());
    }

    @Test
    void unreadableContainerInventoryFailsClosed() throws Exception {
        when(containers.listContainersByLabels(TARGET_CONTAINERS)).thenThrow(new IllegalStateException("private"));
        AwsException preview = assertThrows(AwsException.class, () -> controller.previewAccountReclaim(TARGET));
        AwsException reclaim = assertThrows(AwsException.class, () -> controller.reclaimAccount(TARGET, List.of(TARGET)));
        assertEquals(503, preview.getHttpStatus());
        assertEquals(503, reclaim.getHttpStatus());
        assertEquals("Could not inspect account container dependencies", reclaim.getMessage());
        verifyNoInteractions(storage, services, teardowns);
        assertFalse(activity.isBusy(TARGET));
    }

    @Test
    void anAdmittedOperationBlocksReclaimUntilItReturns() throws Exception {
        try (AccountActivity.Permit ignored = activity.beginWork(TARGET)) {
            assertEquals(new AccountReclaim.Preview(TARGET, false, "account-busy", 0, 0),
                    controller.previewAccountReclaim(TARGET).getEntity());
            assertEquals(409, controller.reclaimAccount(TARGET, List.of(TARGET)).getStatus());
            verifyNoInteractions(guard, containers, storage, services, teardowns);
        }
        assertFalse(activity.isBusy(TARGET));
    }

    @Test
    void reclaimHoldsTheAccountGateThroughNativeClearAndFlush() throws Exception {
        Path target = write("s3/.accounts/" + TARGET + "/bucket/data.s3data", "target");
        Path foreign = write("s3/.accounts/" + FOREIGN + "/bucket/data.s3data", "foreign");
        doAnswer(ignored -> {
            FileTrees.delete(target.getParent().getParent());
            return null;
        }).when(guard).clearAccount(TARGET);
        CountDownLatch flushing = new CountDownLatch(1);
        CountDownLatch releaseFlush = new CountDownLatch(1);
        when(storage.prepareAccountReclaim(TARGET)).thenReturn(() -> {
            flushing.countDown();
            try {
                assertTrue(releaseFlush.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
        });

        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            Future<Response> result = executor.submit(() -> controller.reclaimAccount(TARGET, List.of(TARGET)));
            try {
                assertTrue(flushing.await(10, TimeUnit.SECONDS));
                assertTrue(activity.isReclaiming(TARGET));
                assertEquals(409, assertThrows(AwsException.class, () -> activity.beginWork(TARGET)).getHttpStatus());
                try (AccountActivity.Permit foreignWork = activity.beginWork(FOREIGN)) {
                    assertTrue(activity.isBusy(FOREIGN));
                }
            } finally {
                releaseFlush.countDown();
            }
            assertEquals(200, result.get(10, TimeUnit.SECONDS).getStatus());
        }

        assertFalse(activity.isBusy(TARGET));
        assertFalse(Files.exists(target));
        assertEquals("foreign", Files.readString(foreign));
        verify(storage).prepareAccountReclaim(TARGET);
        verify(storage, never()).clearAccount(TARGET);
        verifyNoInteractions(teardowns);
    }

    @Test
    void cacheBootstrappedAfterTheFinalServiceSnapshotIsClearedBeforeAdmissionResumes() throws Exception {
        ServiceConfigAccess modes = mock(ServiceConfigAccess.class);
        when(modes.storageMode("fixture")).thenReturn("memory");
        StorageFactory factory = new StorageFactory(config, modes);
        AccountAwareStorageBackend<String> backend = factory.create("fixture", "cache.json", new TypeReference<>() {});
        backend.putForAccount(TARGET, "same", "target");
        backend.putForAccount(FOREIGN, "same", "foreign");
        EmulatorInfoController reclaimable = controllerUsing(factory);
        AtomicReference<List<String>> cache = new AtomicReference<>();
        AtomicInteger inventories = new AtomicInteger();
        when(containers.listContainersByLabels(TARGET_CONTAINERS)).thenAnswer(ignored -> {
            if (inventories.incrementAndGet() == 2) {
                factory.initializeAccountCache(() -> {
                    // The final service snapshot has already been read. Simulate a cache restoring
                    // before its owning bean is published for inspection.
                    cache.set(List.of(backend.getForAccount(TARGET, "same").orElseThrow(),
                            backend.getForAccount(FOREIGN, "same").orElseThrow()));
                    return null;
                }, accountId -> {
                    assertEquals(TARGET, accountId);
                    assertTrue(activity.isReclaiming(TARGET));
                    assertTrue(backend.getForAccount(TARGET, "same").isEmpty());
                    cache.updateAndGet(values -> values.stream().filter(value -> !value.equals("target")).toList());
                });
            }
            return List.of();
        });

        assertEquals(200, reclaimable.reclaimAccount(TARGET, List.of(TARGET)).getStatus());

        assertEquals(List.of("foreign"), cache.get());
        assertFalse(activity.isBusy(TARGET));
    }

    @Test
    void dormantAccountReclaimPreservesForeignMultipartAndColdMetadata() throws Exception {
        live.set(List.of());
        StorageFactory factory = new StorageFactory(config, mock(ServiceConfigAccess.class));
        EmulatorInfoController cold = controllerUsing(factory);
        Path s3 = write("s3/.accounts/" + TARGET + "/bucket/object.s3data", "remove");
        Path code = write("lambda-code/" + TARGET + "/fn/file", "remove");
        Path multipart = write("s3/.multipart/foreign-upload/part", "preserve");
        Path foreign = write("s3/.accounts/" + FOREIGN + "/bucket/object.s3data", "preserve");
        Path buckets = write("s3-buckets.json", "{\"" + TARGET + "/target\":{\"name\":\"target\"},\""
                + FOREIGN + "/foreign\":{\"name\":\"foreign\"}}");
        Path unrelated = write("opaque.json", "{\"" + FOREIGN + "/key\":{\"value\":1}}");
        String bytes = Files.readString(unrelated);

        assertEquals(200, cold.reclaimAccount(TARGET, List.of(TARGET)).getStatus());

        assertFalse(Files.exists(s3));
        assertFalse(Files.exists(code));
        assertEquals("preserve", Files.readString(multipart));
        assertEquals("preserve", Files.readString(foreign));
        assertEquals(bytes, Files.readString(unrelated));
        assertFalse(Files.exists(directory.resolve("opaque.wal")));
        assertEquals("{\"" + FOREIGN + "/foreign\":{\"name\":\"foreign\"}}", Files.readString(buckets));
        verifyNoInteractions(services, teardowns);
    }

    @Test
    void dormantUnreadableStoreIsLeftInPlaceBeforePayloadClear() throws Exception {
        live.set(List.of());
        StorageFactory factory = new StorageFactory(config, mock(ServiceConfigAccess.class));
        EmulatorInfoController cold = controllerUsing(factory);
        Path payload = write("s3/.accounts/" + TARGET + "/bucket/object.s3data", "intact");
        Path invalid = write("unopened.json", "{invalid");
        List<Path> before = entries();

        assertThrows(IOException.class, () -> cold.reclaimAccount(TARGET, List.of(TARGET)));
        assertEquals(before, entries());
        assertEquals("intact", Files.readString(payload));
        assertEquals("{invalid", Files.readString(invalid));
        verifyNoInteractions(services, teardowns);
    }

    @Test
    void malformedDormantShardStopsBeforeAnyDeletesOrRecovery() throws Exception {
        live.set(List.of());
        StorageFactory factory = new StorageFactory(config, mock(ServiceConfigAccess.class));
        EmulatorInfoController cold = controllerUsing(factory);
        Path payload = write("s3/.accounts/" + TARGET + "/bucket/object.s3data", "intact");
        String contents = "{\"key\":\"" + TARGET + "/table\",\"entries\":null}";
        Path invalid = write("dynamodb-items/dormant.json", contents);
        List<Path> before = entries();

        assertThrows(IOException.class, () -> cold.reclaimAccount(TARGET, List.of(TARGET)));
        assertEquals(before, entries());
        assertEquals("intact", Files.readString(payload));
        assertEquals(contents, Files.readString(invalid));
        verifyNoInteractions(services, teardowns);
    }

    @Test
    @SuppressWarnings("unchecked")
    void inspectionDoesNotResolveDormantCdiHandles() {
        Instance.Handle<Resettable> live = mock(Instance.Handle.class);
        Instance.Handle<Resettable> dormant = mock(Instance.Handle.class);
        Bean<Resettable> liveBean = mock(Bean.class);
        Bean<Resettable> dormantBean = mock(Bean.class);
        InjectableContext context = mock(InjectableContext.class);
        when(services.handles()).thenAnswer(ignored -> List.of(live, dormant));
        when(live.getBean()).thenReturn(liveBean);
        when(dormant.getBean()).thenReturn(dormantBean);
        when(liveBean.getScope()).thenAnswer(ignored -> ApplicationScoped.class);
        when(dormantBean.getScope()).thenAnswer(ignored -> ApplicationScoped.class);
        when(context.get(liveBean)).thenReturn(guard);

        assertEquals(List.of(guard), AccountReclaim.existingServices(services, ignored -> context));
        verify(live, never()).get();
        verify(dormant, never()).get();
        verify(context).get(dormantBean);
    }

    private Path write(String name, String value) throws IOException {
        Path file = directory.resolve(name);
        Files.createDirectories(file.getParent());
        return Files.writeString(file, value);
    }

    private List<Path> entries() throws IOException {
        try (Stream<Path> walk = Files.walk(directory)) {
            return walk.sorted().toList();
        }
    }
}
