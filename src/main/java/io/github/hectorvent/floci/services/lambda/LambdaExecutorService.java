package io.github.hectorvent.floci.services.lambda;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.services.lambda.LambdaAsyncDestinations.Condition;
import io.github.hectorvent.floci.services.lambda.launcher.ContainerHandle;
import io.github.hectorvent.floci.services.lambda.model.AsyncInvokePolicy;
import io.github.hectorvent.floci.services.lambda.model.InvokeResult;
import io.github.hectorvent.floci.services.lambda.model.InvocationType;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import io.github.hectorvent.floci.services.lambda.model.PendingInvocation;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Orchestrates Lambda function invocations.
 * Handles RequestResponse (sync), Event (async with retries and destinations), and DryRun modes.
 */
@ApplicationScoped
public class LambdaExecutorService {

    private static final Logger LOG = Logger.getLogger(LambdaExecutorService.class);
    /** Extra time for a newly started runtime to request its first invocation. */
    private static final int RUNTIME_DISPATCH_GRACE_SECONDS = 2;

    private final WarmPool warmPool;
    private final ObjectMapper objectMapper;
    private final LambdaConcurrencyLimiter concurrencyLimiter;
    private final LambdaAsyncDestinations asyncDestinations;
    private final long asyncRetryBaseDelayMs;
    private final ScheduledExecutorService asyncRetryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "lambda-async-retry");
        t.setDaemon(true);
        return t;
    });
    private final ExecutorService asyncExecutor = new ThreadPoolExecutor(
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
            Math.max(8, Runtime.getRuntime().availableProcessors() * 4),
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(500),
            new ThreadPoolExecutor.CallerRunsPolicy());

    @Inject
    public LambdaExecutorService(WarmPool warmPool,
                                 ObjectMapper objectMapper,
                                 LambdaConcurrencyLimiter concurrencyLimiter,
                                 LambdaAsyncDestinations asyncDestinations,
                                 EmulatorConfig config) {
        this(warmPool, objectMapper, concurrencyLimiter, asyncDestinations,
                config.services().lambda().asyncRetryBaseDelayMs());
    }

    LambdaExecutorService(WarmPool warmPool,
                          ObjectMapper objectMapper,
                          LambdaConcurrencyLimiter concurrencyLimiter,
                          LambdaAsyncDestinations asyncDestinations,
                          long asyncRetryBaseDelayMs) {
        this.warmPool = warmPool;
        this.objectMapper = objectMapper;
        this.concurrencyLimiter = concurrencyLimiter;
        this.asyncDestinations = asyncDestinations;
        this.asyncRetryBaseDelayMs = asyncRetryBaseDelayMs;
    }

    /**
     * Invokes {@code fn}. An Event invocation runs with AWS's default asynchronous policy; use
     * {@link #invokeAsync} to apply the function's event-invoke config.
     */
    public InvokeResult invoke(LambdaFunction fn, byte[] payload, InvocationType type) {
        if (type == InvocationType.Event) {
            return invokeAsync(fn, payload, AsyncInvokePolicy.from(qualifiedArn(fn), null));
        }
        String requestId = UUID.randomUUID().toString();

        if (type == InvocationType.DryRun) {
            return new InvokeResult(204, null, new byte[0], null, requestId);
        }

        LambdaConcurrencyLimiter.Permit permit = concurrencyLimiter.acquire(fn);
        try {
            return executeSync(fn, payload, requestId);
        } finally {
            permit.close();
        }
    }

    /**
     * Accepts an asynchronous invocation and returns 202. A failed attempt (function error,
     * timeout, or init failure) is retried up to {@link AsyncInvokePolicy#maximumRetryAttempts()}
     * times, after the configured base delay and then double it for each later retry; a retry
     * due once the event is older than {@link AsyncInvokePolicy#maximumEventAgeSeconds()} is
     * dropped instead. The outcome is recorded to the policy's OnSuccess or OnFailure destination.
     */
    public InvokeResult invokeAsync(LambdaFunction fn, byte[] payload, AsyncInvokePolicy policy) {
        String requestId = UUID.randomUUID().toString();
        LambdaConcurrencyLimiter.Permit permit = concurrencyLimiter.acquire(fn);
        AsyncEvent event = new AsyncEvent(fn, payload, requestId, policy, System.currentTimeMillis());
        try {
            asyncExecutor.submit(() -> runAsyncAttempt(event, 1, permit));
        } catch (RuntimeException e) {
            permit.close();
            throw e;
        }
        return new InvokeResult(202, null, new byte[0], null, requestId);
    }

    private record AsyncEvent(LambdaFunction fn, byte[] payload, String requestId,
                              AsyncInvokePolicy policy, long acceptedAtMs) {}

    private void runAsyncAttempt(AsyncEvent event, int attempt, LambdaConcurrencyLimiter.Permit permit) {
        InvokeResult result;
        try {
            result = executeSync(event.fn(), event.payload(), event.requestId());
        } finally {
            permit.close();
        }
        result.setExecutedVersion(event.fn().getVersion() != null ? event.fn().getVersion() : "$LATEST");

        if (result.getFunctionError() == null) {
            asyncDestinations.deliver(event.policy().onSuccessDestination(), Condition.Success,
                    event.policy().invokedFunctionArn(), event.payload(), result, attempt);
            return;
        }
        if (attempt > event.policy().maximumRetryAttempts()) {
            LOG.warnv("Async invocation {0} of {1} failed after {2} attempts; discarding the event",
                    event.requestId(), event.policy().invokedFunctionArn(), attempt);
            asyncDestinations.deliver(event.policy().onFailureDestination(), Condition.RetriesExhausted,
                    event.policy().invokedFunctionArn(), event.payload(), result, attempt);
            return;
        }
        long delayMs = asyncRetryBaseDelayMs << Math.min(attempt - 1, 20);
        LOG.infov("Async invocation {0} of {1} failed on attempt {2}; retrying in {3} ms",
                event.requestId(), event.policy().invokedFunctionArn(), attempt, delayMs);
        scheduleAsyncRetry(event, attempt + 1, result, delayMs);
    }

    private void scheduleAsyncRetry(AsyncEvent event, int attempt, InvokeResult lastResult, long delayMs) {
        asyncRetryScheduler.schedule(() -> startAsyncRetry(event, attempt, lastResult),
                delayMs, TimeUnit.MILLISECONDS);
    }

    private void startAsyncRetry(AsyncEvent event, int attempt, InvokeResult lastResult) {
        long ageMs = System.currentTimeMillis() - event.acceptedAtMs();
        if (ageMs >= event.policy().maximumEventAgeSeconds() * 1000L) {
            LOG.warnv("Async invocation {0} of {1} exceeded its maximum event age of {2}s; discarding the event",
                    event.requestId(), event.policy().invokedFunctionArn(), event.policy().maximumEventAgeSeconds());
            asyncDestinations.deliver(event.policy().onFailureDestination(), Condition.EventAgeExceeded,
                    event.policy().invokedFunctionArn(), event.payload(), lastResult, attempt - 1);
            return;
        }
        LambdaConcurrencyLimiter.Permit permit;
        try {
            permit = concurrencyLimiter.acquire(event.fn());
        } catch (AwsException throttled) {
            // A throttled retry waits and tries again without spending an attempt, as AWS does.
            scheduleAsyncRetry(event, attempt, lastResult, asyncRetryBaseDelayMs);
            return;
        }
        try {
            asyncExecutor.submit(() -> runAsyncAttempt(event, attempt, permit));
        } catch (RuntimeException e) {
            permit.close();
            LOG.errorv("Async invocation {0} of {1}: could not schedule attempt {2}: {3}",
                    event.requestId(), event.policy().invokedFunctionArn(), attempt, e.getMessage());
        }
    }

    private static String qualifiedArn(LambdaFunction fn) {
        String version = fn.getVersion();
        if (version == null || "$LATEST".equals(version)) {
            return fn.getFunctionArn() + ":$LATEST";
        }
        return fn.getFunctionArn();
    }

    private InvokeResult executeSync(LambdaFunction fn, byte[] payload, String requestId) {
        ContainerHandle handle;
        try {
            handle = warmPool.acquire(fn);
        } catch (LambdaEnvironmentLimiter.AdmissionTimeoutException e) {
            LOG.warnv("Physical environment admission timed out for function {0}: {1}",
                    fn.getFunctionName(), e.getMessage());
            return new InvokeResult(200, "Unhandled",
                    buildErrorPayload(e.getMessage(), "Lambda.EnvironmentTimeout"),
                    null, requestId);
        } catch (LambdaEnvironmentLimiter.AdmissionInterruptedException e) {
            // LambdaEnvironmentLimiter restores the interrupted flag before exposing this
            // unchecked exception, so preserve it while returning a bounded invocation error.
            LOG.warnv("Physical environment admission interrupted for function {0}", fn.getFunctionName());
            return new InvokeResult(200, "Unhandled",
                    buildErrorPayload("Invocation interrupted while waiting for a Lambda execution environment",
                            "Interrupted"),
                    null, requestId);
        } catch (LambdaEnvironmentLimiter.AdmissionClosedException e) {
            LOG.warnv("Physical environment admission is closed for function {0}", fn.getFunctionName());
            return new InvokeResult(200, "Unhandled",
                    buildErrorPayload(e.getMessage(), "Lambda.EnvironmentUnavailable"),
                    null, requestId);
        } catch (Exception e) {
            LOG.warnv("Failed to acquire container for function {0}: {1}", fn.getFunctionName(), e.getMessage());
            return new InvokeResult(200, "Unhandled",
                    buildErrorPayload("Failed to start Lambda container: " + e.getMessage(), "Lambda.InitError"),
                    null, requestId);
        }
        try {
            long deadlineMs = System.currentTimeMillis() + (long) fn.getTimeout() * 1000;
            PendingInvocation invocation = new PendingInvocation(
                    requestId, payload, deadlineMs, fn.getFunctionArn(),
                    new CompletableFuture<>());

            handle.getRuntimeApiServer().enqueue(invocation);

            CompletableFuture.anyOf(
                            invocation.getDispatchedFuture(), invocation.getResultFuture())
                    .get(fn.getTimeout() + RUNTIME_DISPATCH_GRACE_SECONDS, TimeUnit.SECONDS);
            InvokeResult result = invocation.getResultFuture().get();

            warmPool.release(handle);
            return result;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            warmPool.destroyHandle(handle);
            return new InvokeResult(200, "Unhandled", buildErrorPayload("Invocation interrupted", "Interrupted"), null, requestId);
        } catch (Exception e) {
            Throwable cause = e instanceof ExecutionException && e.getCause() != null ? e.getCause() : e;
            if (cause instanceof TimeoutException) {
                LOG.warnv("Function {0} timed out after {1}s", fn.getFunctionName(), fn.getTimeout());
                warmPool.destroyHandle(handle);
                return new InvokeResult(200, "Unhandled",
                        buildErrorPayload("Task timed out after " + fn.getTimeout() + " seconds", "Function.TimedOut"),
                        null, requestId);
            }
            LOG.warnv("Invocation error for function {0}: {1}", fn.getFunctionName(), cause.getMessage());
            warmPool.destroyHandle(handle);
            return new InvokeResult(200, "Unhandled",
                    buildErrorPayload(cause.getMessage(), "InvocationError"), null, requestId);
        }
    }

    @PreDestroy
    public void shutdown() {
        asyncRetryScheduler.shutdownNow();
        asyncExecutor.shutdownNow();
    }

    private byte[] buildErrorPayload(String message, String errorType) {
        try {
            ObjectNode node = objectMapper.createObjectNode();
            node.put("errorMessage", message);
            node.put("errorType", errorType);
            return objectMapper.writeValueAsBytes(node);
        } catch (Exception e) {
            return ("{\"errorMessage\":\"unknown\",\"errorType\":\"" + errorType + "\"}").getBytes();
        }
    }
}
