package io.github.hectorvent.floci.services.lambda;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.services.lambda.launcher.ContainerHandle;
import io.github.hectorvent.floci.services.lambda.model.AsyncInvokePolicy;
import io.github.hectorvent.floci.services.lambda.model.ContainerState;
import io.github.hectorvent.floci.services.lambda.model.InvokeResult;
import io.github.hectorvent.floci.services.lambda.model.LambdaFunction;
import io.github.hectorvent.floci.services.lambda.model.PendingInvocation;
import io.github.hectorvent.floci.services.lambda.runtime.RuntimeApiServer;
import io.github.hectorvent.floci.services.sqs.SqsService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Asynchronous (Event) invocation retries and OnSuccess/OnFailure destination delivery, driven
 * through {@link LambdaExecutorService} with a stub runtime and a recording SQS service.
 */
class LambdaAsyncInvocationTest {

    private static final String FUNCTION_ARN = "arn:aws:lambda:us-east-1:000000000000:function:async-fn";
    private static final String INVOKED_ARN = FUNCTION_ARN + ":$LATEST";
    private static final String FAILURE_QUEUE_ARN = "arn:aws:sqs:us-east-1:000000000000:async-failures";
    private static final String SUCCESS_QUEUE_ARN = "arn:aws:sqs:us-east-1:000000000000:async-successes";
    private static final String BASE_URL = "http://localhost:4566";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicInteger attempts = new AtomicInteger();
    private final List<String[]> sent = new CopyOnWriteArrayList<>();
    private final CountDownLatch delivered = new CountDownLatch(1);

    private WarmPool warmPool;
    private SqsService sqsService;
    private LambdaFunction fn;
    private LambdaExecutorService executor;

    @BeforeEach
    void setUp() {
        warmPool = mock(WarmPool.class);
        sqsService = mock(SqsService.class);
        LambdaConcurrencyLimiter concurrencyLimiter = mock(LambdaConcurrencyLimiter.class);
        when(concurrencyLimiter.acquire(any())).thenReturn(() -> {});
        doAnswer(inv -> {
            sent.add(new String[] {inv.getArgument(0), inv.getArgument(1), inv.getArgument(3)});
            delivered.countDown();
            return null;
        }).when(sqsService).sendMessage(anyString(), anyString(), anyInt(), anyString());

        fn = new LambdaFunction();
        fn.setFunctionName("async-fn");
        fn.setFunctionArn(FUNCTION_ARN);
        fn.setVersion("$LATEST");
        fn.setTimeout(3);

        executor = new LambdaExecutorService(warmPool, objectMapper, concurrencyLimiter,
                new LambdaAsyncDestinations(sqsService, objectMapper, BASE_URL), 50);
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
    }

    @Test
    void failingEventIsRetriedThenRecordedToOnFailureQueue() throws Exception {
        runtimeReturns(new InvokeResult(200, "Unhandled",
                "{\"errorMessage\":\"no handler claimed the event\",\"errorType\":\"Error\"}".getBytes(),
                null, null));

        InvokeResult accepted = executor.invokeAsync(fn, "{\"probe\":true}".getBytes(),
                new AsyncInvokePolicy(INVOKED_ARN, 2, 3600, null, FAILURE_QUEUE_ARN));

        assertEquals(202, accepted.getStatusCode());
        assertTrue(delivered.await(5, TimeUnit.SECONDS), "no OnFailure record was delivered");
        assertEquals(3, attempts.get());
        assertEquals(1, sent.size());
        assertEquals(BASE_URL + "/000000000000/async-failures", sent.get(0)[0]);
        assertEquals("us-east-1", sent.get(0)[2]);

        JsonNode record = objectMapper.readTree(sent.get(0)[1]);
        assertEquals("1.0", record.path("version").asText());
        assertFalse(record.path("timestamp").asText().isEmpty());
        JsonNode requestContext = record.path("requestContext");
        assertEquals(accepted.getRequestId(), requestContext.path("requestId").asText());
        assertEquals(INVOKED_ARN, requestContext.path("functionArn").asText());
        assertEquals("RetriesExhausted", requestContext.path("condition").asText());
        assertEquals(3, requestContext.path("approximateInvokeCount").asInt());
        assertTrue(record.path("requestPayload").path("probe").asBoolean());
        JsonNode responseContext = record.path("responseContext");
        assertEquals(200, responseContext.path("statusCode").asInt());
        assertEquals("$LATEST", responseContext.path("executedVersion").asText());
        assertEquals("Unhandled", responseContext.path("functionError").asText());
        assertEquals("no handler claimed the event", record.path("responsePayload").path("errorMessage").asText());
    }

    @Test
    void zeroRetriesRecordsAfterTheFirstAttempt() throws Exception {
        runtimeReturns(new InvokeResult(200, "Unhandled", "{\"errorMessage\":\"x\"}".getBytes(), null, null));

        executor.invokeAsync(fn, "{}".getBytes(), new AsyncInvokePolicy(INVOKED_ARN, 0, 3600, null, FAILURE_QUEUE_ARN));

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        assertEquals(1, attempts.get());
        JsonNode requestContext = objectMapper.readTree(sent.get(0)[1]).path("requestContext");
        assertEquals("RetriesExhausted", requestContext.path("condition").asText());
        assertEquals(1, requestContext.path("approximateInvokeCount").asInt());
    }

    @Test
    void retryDueAfterMaximumEventAgeIsDroppedAsEventAgeExceeded() throws Exception {
        runtimeReturns(new InvokeResult(200, "Unhandled", "{\"errorMessage\":\"x\"}".getBytes(), null, null));
        LambdaConcurrencyLimiter concurrencyLimiter = mock(LambdaConcurrencyLimiter.class);
        when(concurrencyLimiter.acquire(any())).thenReturn(() -> {});
        executor.shutdown();
        executor = new LambdaExecutorService(warmPool, objectMapper, concurrencyLimiter,
                new LambdaAsyncDestinations(sqsService, objectMapper, BASE_URL), 1_200);

        executor.invokeAsync(fn, "{}".getBytes(), new AsyncInvokePolicy(INVOKED_ARN, 2, 1, null, FAILURE_QUEUE_ARN));

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        assertEquals(1, attempts.get());
        JsonNode record = objectMapper.readTree(sent.get(0)[1]);
        assertEquals("EventAgeExceeded", record.path("requestContext").path("condition").asText());
        assertEquals(1, record.path("requestContext").path("approximateInvokeCount").asInt());
        assertEquals("Unhandled", record.path("responseContext").path("functionError").asText());
    }

    @Test
    void successfulEventIsRecordedToOnSuccessQueueOnly() throws Exception {
        runtimeReturns(new InvokeResult(200, null, "{\"ok\":true}".getBytes(), null, null));

        executor.invokeAsync(fn, "{}".getBytes(),
                new AsyncInvokePolicy(INVOKED_ARN, 2, 3600, SUCCESS_QUEUE_ARN, FAILURE_QUEUE_ARN));

        assertTrue(delivered.await(5, TimeUnit.SECONDS));
        assertEquals(1, attempts.get());
        assertEquals(1, sent.size());
        assertEquals(BASE_URL + "/000000000000/async-successes", sent.get(0)[0]);
        JsonNode record = objectMapper.readTree(sent.get(0)[1]);
        assertEquals("Success", record.path("requestContext").path("condition").asText());
        assertFalse(record.path("responseContext").has("functionError"));
        assertTrue(record.path("responsePayload").path("ok").asBoolean());
    }

    @Test
    void failingEventWithoutDestinationIsRetriedAndDiscarded() throws Exception {
        CountDownLatch thirdAttempt = new CountDownLatch(3);
        runtimeReturns(new InvokeResult(200, "Unhandled", "{\"errorMessage\":\"x\"}".getBytes(), null, null),
                thirdAttempt);

        executor.invokeAsync(fn, "{}".getBytes(), AsyncInvokePolicy.from(INVOKED_ARN, null));

        assertTrue(thirdAttempt.await(5, TimeUnit.SECONDS), "AWS's default of two retries was not applied");
        Thread.sleep(300);
        assertEquals(3, attempts.get());
        verify(sqsService, never()).sendMessage(anyString(), anyString(), anyInt(), anyString());
    }

    @Test
    void unsupportedDestinationTypeIsSkipped() throws Exception {
        CountDownLatch attempted = new CountDownLatch(1);
        runtimeReturns(new InvokeResult(200, "Unhandled", "{\"errorMessage\":\"x\"}".getBytes(), null, null),
                attempted);

        executor.invokeAsync(fn, "{}".getBytes(), new AsyncInvokePolicy(INVOKED_ARN, 0, 3600, null,
                "arn:aws:events:us-east-1:000000000000:event-bus/default"));

        assertTrue(attempted.await(5, TimeUnit.SECONDS));
        Thread.sleep(200);
        verify(sqsService, never()).sendMessage(anyString(), anyString(), anyInt(), anyString());
    }

    @Test
    void nonJsonPayloadsAreRecordedAsStrings() throws Exception {
        LambdaAsyncDestinations destinations = new LambdaAsyncDestinations(sqsService, objectMapper, BASE_URL);
        InvokeResult result = new InvokeResult(200, "Unhandled", "plain failure".getBytes(), null, "req-1");
        result.setExecutedVersion("7");

        JsonNode record = objectMapper.readTree(destinations.buildRecord(
                LambdaAsyncDestinations.Condition.RetriesExhausted, FUNCTION_ARN + ":7",
                "not json".getBytes(), result, 3));

        assertEquals("not json", record.path("requestPayload").asText());
        assertEquals("plain failure", record.path("responsePayload").asText());
        assertEquals("7", record.path("responseContext").path("executedVersion").asText());
        assertEquals("req-1", record.path("requestContext").path("requestId").asText());
    }

    private void runtimeReturns(InvokeResult result) {
        runtimeReturns(result, new CountDownLatch(0));
    }

    private void runtimeReturns(InvokeResult result, CountDownLatch onAttempt) {
        RuntimeApiServer runtime = mock(RuntimeApiServer.class);
        when(warmPool.acquire(eq(fn))).thenAnswer(inv ->
                new ContainerHandle("cid-" + attempts.get(), "async-fn", runtime, ContainerState.WARM));
        doAnswer(inv -> {
            PendingInvocation invocation = inv.getArgument(0);
            attempts.incrementAndGet();
            InvokeResult attemptResult = new InvokeResult(result.getStatusCode(), result.getFunctionError(),
                    result.getPayload(), null, invocation.getRequestId());
            invocation.getResultFuture().complete(attemptResult);
            onAttempt.countDown();
            return invocation.getResultFuture();
        }).when(runtime).enqueue(any(PendingInvocation.class));
    }
}
