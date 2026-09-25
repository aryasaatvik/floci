package io.github.hectorvent.floci.services.lambda;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.services.lambda.model.InvokeResult;
import io.github.hectorvent.floci.services.sqs.SqsService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Builds and delivers the invocation records Lambda sends to an asynchronous invocation's
 * {@code OnSuccess} and {@code OnFailure} destinations.
 */
@ApplicationScoped
public class LambdaAsyncDestinations {

    private static final Logger LOG = Logger.getLogger(LambdaAsyncDestinations.class);

    /** Why an event reached a destination; the values are AWS's {@code requestContext.condition}. */
    public enum Condition {
        Success,
        RetriesExhausted,
        EventAgeExceeded
    }

    private final SqsService sqsService;
    private final ObjectMapper objectMapper;
    private final String baseUrl;

    @Inject
    public LambdaAsyncDestinations(SqsService sqsService, ObjectMapper objectMapper, EmulatorConfig config) {
        this(sqsService, objectMapper, config.effectiveBaseUrl());
    }

    LambdaAsyncDestinations(SqsService sqsService, ObjectMapper objectMapper, String baseUrl) {
        this.sqsService = sqsService;
        this.objectMapper = objectMapper;
        this.baseUrl = baseUrl;
    }

    /**
     * Sends one invocation record to {@code destinationArn}. SQS is supported; any other
     * destination type is logged and skipped. A delivery failure is logged, never thrown: AWS
     * reports it only through the {@code DestinationDeliveryFailures} metric.
     */
    public void deliver(String destinationArn, Condition condition, String invokedFunctionArn, byte[] requestPayload,
                        InvokeResult lastResult, int approximateInvokeCount) {
        if (destinationArn == null || destinationArn.isBlank()) {
            return;
        }
        try {
            String record = buildRecord(condition, invokedFunctionArn, requestPayload, lastResult,
                    approximateInvokeCount);
            if (destinationArn.contains(":sqs:")) {
                String queueUrl = AwsArnUtils.arnToQueueUrl(destinationArn, baseUrl);
                sqsService.sendMessage(queueUrl, record, 0, AwsArnUtils.parse(destinationArn).region());
                LOG.infov("Async invocation {0} of {1}: sent {2} record to {3}",
                        lastResult != null ? lastResult.getRequestId() : "", invokedFunctionArn, condition,
                        destinationArn);
            } else {
                LOG.warnv("Async invocation of {0}: destination type of {1} is not supported; {2} record dropped",
                        invokedFunctionArn, destinationArn, condition);
            }
        } catch (RuntimeException | JsonProcessingException e) {
            LOG.errorv("Async invocation of {0}: failed to deliver {1} record to {2}: {3}",
                    invokedFunctionArn, condition, destinationArn, e.getMessage());
        }
    }

    String buildRecord(Condition condition, String invokedFunctionArn, byte[] requestPayload,
                       InvokeResult lastResult, int approximateInvokeCount) throws JsonProcessingException {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("version", "1.0");
        root.put("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

        ObjectNode requestContext = root.putObject("requestContext");
        requestContext.put("requestId", lastResult != null ? lastResult.getRequestId() : null);
        requestContext.put("functionArn", invokedFunctionArn);
        requestContext.put("condition", condition.name());
        requestContext.put("approximateInvokeCount", approximateInvokeCount);

        root.set("requestPayload", parsePayload(requestPayload));

        if (lastResult != null) {
            ObjectNode responseContext = root.putObject("responseContext");
            responseContext.put("statusCode", lastResult.getStatusCode());
            responseContext.put("executedVersion", lastResult.getExecutedVersion());
            if (lastResult.getFunctionError() != null) {
                responseContext.put("functionError", lastResult.getFunctionError());
            }
            root.set("responsePayload", parsePayload(lastResult.getPayload()));
        }
        return objectMapper.writeValueAsString(root);
    }

    /** A JSON payload is embedded as JSON; anything else is embedded as a string, as AWS does. */
    private JsonNode parsePayload(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            return objectMapper.readTree(payload);
        } catch (Exception expected) {
            // A non-JSON payload is valid for Invoke; AWS records it verbatim as a string.
            return TextNode.valueOf(new String(payload, StandardCharsets.UTF_8));
        }
    }
}
