package io.github.hectorvent.floci.services.sqs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.services.lambda.LambdaAsyncDestinations;
import io.github.hectorvent.floci.services.lambda.model.InvokeResult;
import io.github.hectorvent.floci.services.sqs.model.Message;
import io.github.hectorvent.floci.services.sqs.model.Queue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An asynchronous invocation's OnFailure record reaches a real SQS queue from a background
 * thread, outside any request scope, the way the Lambda retry scheduler delivers it.
 */
class LambdaAsyncDestinationSqsDeliveryTest {

    private static final String BASE_URL = "http://localhost:4566";
    private static final String ACCOUNT = "000000000000";
    private static final String REGION = "eu-west-1";

    @Test
    void onFailureRecordLandsOnTheDestinationQueue() throws Exception {
        SqsService sqsService = new SqsService(new InMemoryStorage<>(), 30, 1048576, BASE_URL);
        Queue queue = sqsService.createQueue("async-failures", null, REGION);
        String destinationArn = "arn:aws:sqs:" + REGION + ":" + ACCOUNT + ":async-failures";

        EmulatorConfig config = mock(EmulatorConfig.class);
        when(config.effectiveBaseUrl()).thenReturn(BASE_URL);
        ObjectMapper objectMapper = new ObjectMapper();
        LambdaAsyncDestinations destinations = new LambdaAsyncDestinations(sqsService, objectMapper, config);
        InvokeResult lastResult = new InvokeResult(200, "Unhandled",
                "{\"errorMessage\":\"boom\"}".getBytes(), null, "req-42");
        lastResult.setExecutedVersion("$LATEST");

        CompletableFuture.runAsync(() -> destinations.deliver(destinationArn,
                LambdaAsyncDestinations.Condition.RetriesExhausted,
                "arn:aws:lambda:" + REGION + ":" + ACCOUNT + ":function:fn:$LATEST",
                "{\"probe\":true}".getBytes(), lastResult, 3)).get();

        List<Message> messages = sqsService.receiveMessage(queue.getQueueUrl(), 10, 30, 0, REGION);
        assertEquals(1, messages.size());
        JsonNode record = objectMapper.readTree(messages.get(0).getBody());
        assertEquals("RetriesExhausted", record.path("requestContext").path("condition").asText());
        assertEquals(3, record.path("requestContext").path("approximateInvokeCount").asInt());
        assertEquals("req-42", record.path("requestContext").path("requestId").asText());
        assertEquals("boom", record.path("responsePayload").path("errorMessage").asText());
    }
}
