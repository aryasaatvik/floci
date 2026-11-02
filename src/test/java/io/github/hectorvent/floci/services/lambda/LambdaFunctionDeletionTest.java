package io.github.hectorvent.floci.services.lambda;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.services.lambda.durable.DurableFunctionInvoker.ResolvedDurableTarget;
import io.github.hectorvent.floci.services.lambda.durable.LambdaDurableFunctionInvoker;
import io.github.hectorvent.floci.services.lambda.launcher.ContainerHandle;
import io.github.hectorvent.floci.services.lambda.launcher.LambdaRuntimeLauncher;
import io.github.hectorvent.floci.services.lambda.model.ContainerState;
import io.github.hectorvent.floci.services.lambda.model.InvocationType;
import io.github.hectorvent.floci.services.lambda.model.InvokeResult;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import io.github.hectorvent.floci.services.lambda.model.PendingInvocation;
import io.github.hectorvent.floci.services.lambda.runtime.RuntimeApiServer;
import io.github.hectorvent.floci.services.lambda.zip.CodeStore;
import io.github.hectorvent.floci.services.lambda.zip.ZipExtractor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.hectorvent.floci.testing.ThreadStates.awaitBlockedOrDone;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class LambdaFunctionDeletionTest {

    private static final String ACCOUNT = "000000000000";
    private static final String REGION = "us-east-1";
    private static final String NAME = "workflow";

    @TempDir Path codeDirectory;
    private final LambdaRuntimeLauncher launcher = mock(LambdaRuntimeLauncher.class);
    private final LambdaFunctionStore functions = new LambdaFunctionStore(new InMemoryStorage<>());
    private final LambdaEnvironmentLimiter limiter = new LambdaEnvironmentLimiter(2, 10);
    private final CountDownLatch continueStop = new CountDownLatch(1);
    private WarmPool pool;
    private LambdaService service;
    private LambdaExecutorService executor;
    private LambdaFunction latest;
    private LambdaFunction version;

    @BeforeEach
    void setUp() {
        EmulatorConfig config = mock(EmulatorConfig.class, RETURNS_DEEP_STUBS);
        when(config.services().lambda().warmPoolMaxPerFunction()).thenReturn(Optional.of(4));
        when(config.services().lambda().containerDrainTimeoutSeconds()).thenReturn(2);
        pool = new WarmPool(launcher, config, Clock.systemUTC(), null, limiter);
        service = new LambdaService(functions, pool, new CodeStore(codeDirectory), new ZipExtractor(),
                new RegionResolver(REGION, ACCOUNT));
        executor = new LambdaExecutorService(pool, new ObjectMapper(), new LambdaConcurrencyLimiter());
        latest = function("$LATEST");
        version = function("1");
    }

    @AfterEach
    void tearDown() {
        continueStop.countDown();
        executor.shutdown();
        pool.shutdown();
    }

    private LambdaFunction function(String number) {
        LambdaFunction fn = LambdaFunctionTestFixtures.function(ACCOUNT, REGION, NAME, number);
        fn.setDurableExecutionTimeout(60);
        functions.save(REGION, fn);
        return fn;
    }

    private static ContainerHandle handle(String id) {
        RuntimeApiServer runtime = mock(RuntimeApiServer.class);
        doAnswer(call -> {
            PendingInvocation invocation = call.getArgument(0);
            invocation.getResultFuture().complete(new InvokeResult(200, null, "{}".getBytes(),
                    null, invocation.getRequestId()));
            return null;
        }).when(runtime).enqueue(any());
        return new ContainerHandle(id, NAME, runtime, ContainerState.WARM);
    }

    private static void missing(Runnable invocation) {
        AwsException failure = assertThrows(AwsException.class, invocation::run);
        assertEquals("ResourceNotFoundException", failure.getErrorCode());
        assertEquals(404, failure.getHttpStatus());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void durableResumeDuringDeleteCannotLaunchAReplacement(boolean qualified) throws Exception {
        ContainerHandle first = handle("original");
        ContainerHandle replacement = handle("replacement");
        when(launcher.launch(version)).thenReturn(first, replacement);
        CountDownLatch stopping = new CountDownLatch(1);
        doAnswer(call -> {
            stopping.countDown();
            assertTrue(continueStop.await(5, TimeUnit.SECONDS));
            first.setState(ContainerState.STOPPED);
            return null;
        }).when(launcher).stop(first);
        LambdaDurableFunctionInvoker invoker = new LambdaDurableFunctionInvoker(
                new LambdaTargetResolver(functions, null), executor);
        ResolvedDurableTarget target = invoker.resolve(ACCOUNT, REGION, NAME, "1");
        LambdaFunction sibling = function("2");
        pool.release(pool.acquire(version));
        CompletableFuture<Void> deletion = CompletableFuture.runAsync(() ->
                service.deleteFunction(REGION, NAME, qualified ? "1" : null));
        try {
            assertTrue(stopping.await(5, TimeUnit.SECONDS));
            assertTrue(functions.get(REGION, NAME, "1").isPresent(), "delete is held inside container stop");
            missing(() -> invoker.invoke(target, "{}".getBytes(), "callback-resume"));
            verify(launcher, times(1)).launch(version);
        } finally {
            continueStop.countDown();
            deletion.get(5, TimeUnit.SECONDS);
        }
        missing(() -> executor.invoke(version, new byte[0], InvocationType.DryRun));
        assertEquals(0, limiter.status().total());
        assertEquals(ContainerState.STOPPED, first.getState());
        if (qualified) {
            assertSame(latest, functions.get(REGION, NAME).orElseThrow());
            when(launcher.launch(sibling)).thenReturn(replacement);
            assertSame(replacement, pool.acquire(sibling));
            pool.release(replacement);
            ContainerHandle latestHandle = handle("latest");
            when(launcher.launch(latest)).thenReturn(latestHandle);
            assertSame(latestHandle, pool.acquire(latest));
        } else {
            LambdaFunction recreated = function("$LATEST");
            when(launcher.launch(recreated)).thenReturn(replacement);
            assertSame(replacement, pool.acquire(recreated));
        }
    }

    @Test
    void launchReturningAfterDeletionIsStoppedAndRejected() throws Exception {
        ContainerHandle late = handle("late");
        CountDownLatch launching = new CountDownLatch(1);
        CountDownLatch continueLaunch = new CountDownLatch(1);
        when(launcher.launch(version)).thenAnswer(call -> {
            launching.countDown();
            assertTrue(continueLaunch.await(5, TimeUnit.SECONDS));
            return late;
        });
        CompletableFuture<ContainerHandle> acquisition = CompletableFuture.supplyAsync(() -> pool.acquire(version));
        assertTrue(launching.await(5, TimeUnit.SECONDS));
        CompletableFuture<Void> deletion = CompletableFuture.runAsync(() -> service.deleteFunction(REGION, NAME));
        try {
            await().atMost(Duration.ofSeconds(5)).until(version::isDeleted);
            assertFalse(deletion.isDone(), "deletion waits for the admitted launch's terminal cleanup");
        } finally {
            continueLaunch.countDown();
        }
        deletion.get(5, TimeUnit.SECONDS);
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> acquisition.get(5, TimeUnit.SECONDS));
        assertInstanceOf(AwsException.class, failure.getCause());
        assertEquals("ResourceNotFoundException", ((AwsException) failure.getCause()).getErrorCode());
        verify(launcher).stop(late);
        assertEquals(0, limiter.status().total());
    }

    @Test
    void deletionWakesAQueuedAdmissionWithoutReleasingAnotherFunctionsSlot() throws Exception {
        LambdaFunction other = LambdaFunctionTestFixtures.function(ACCOUNT, REGION, "other", "$LATEST");
        ContainerHandle busy = handle("busy-other");
        ContainerHandle second = handle("second-busy-other");
        when(launcher.launch(other)).thenReturn(busy, second);
        pool.acquire(other);
        pool.acquire(other);
        CompletableFuture<ContainerHandle> queued = CompletableFuture.supplyAsync(() -> pool.acquire(version));
        await().atMost(Duration.ofSeconds(5)).until(() -> limiter.status().queued() == 1);
        service.deleteFunction(REGION, NAME);
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> queued.get(2, TimeUnit.SECONDS));
        assertInstanceOf(AwsException.class, failure.getCause());
        assertEquals("ResourceNotFoundException", ((AwsException) failure.getCause()).getErrorCode());
        verify(launcher, never()).launch(version);
        verify(launcher, never()).stop(busy);
        verify(launcher, never()).stop(second);
        assertEquals(2, limiter.status().total());
    }

    @Test
    void publicationThatResolvedBeforeDeletionCannotPersistAnOrphanVersion() throws Exception {
        AtomicReference<Thread> publishingThread = new AtomicReference<>();
        CompletableFuture<LambdaFunction> publication;
        synchronized (functions.lockForConcurrencyOp(latest.getFunctionArn())) {
            publication = CompletableFuture.supplyAsync(() -> {
                publishingThread.set(Thread.currentThread());
                return service.publishVersion(REGION, NAME, null);
            });
            awaitBlockedOrDone(publishingThread, publication);
            service.deleteFunction(REGION, NAME);
        }
        ExecutionException failure = assertThrows(ExecutionException.class,
                () -> publication.get(5, TimeUnit.SECONDS));
        assertInstanceOf(AwsException.class, failure.getCause());
        assertEquals("ResourceNotFoundException", ((AwsException) failure.getCause()).getErrorCode());
        assertTrue(functions.listVersions(REGION, NAME).isEmpty());
    }

    @Test
    void deletionPreservesAnotherAccountsSameNamedFunction() {
        LambdaFunction foreign = LambdaFunctionTestFixtures.function("111111111111", REGION, NAME, "1");
        ContainerHandle own = handle("own");
        ContainerHandle other = handle("other-account");
        when(launcher.launch(version)).thenReturn(own);
        when(launcher.launch(foreign)).thenReturn(other);
        when(launcher.isAlive(other)).thenReturn(true);
        pool.acquire(version);
        pool.release(pool.acquire(foreign));
        service.deleteFunction(REGION, NAME);
        verify(launcher).stop(own);
        verify(launcher, never()).stop(other);
        assertSame(other, pool.acquire(foreign));
        assertEquals(1, limiter.status().total());
    }

    @Test
    void deletionStopsBusyContainersAndCanRetryAFailedStop() {
        ContainerHandle busy = handle("busy");
        when(launcher.launch(version)).thenReturn(busy);
        pool.acquire(version);
        doThrow(new IllegalStateException("stop failed")).doNothing().when(launcher).stop(busy);
        service.deleteFunction(REGION, NAME);
        assertEquals(1, limiter.status().retiring());
        pool.evictIdleContainers();
        verify(launcher, times(2)).stop(busy);
        assertEquals(0, limiter.status().total());
    }
}
