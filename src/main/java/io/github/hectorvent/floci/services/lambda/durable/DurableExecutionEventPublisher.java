package io.github.hectorvent.floci.services.lambda.durable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.hectorvent.floci.services.eventbridge.EventBridgeService;
import io.github.hectorvent.floci.services.lambda.durable.model.DurableExecution;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Publishes AWS-shaped {@code aws.lambda} "Durable Execution Status Change" events to the
 * execution's account and Region default bus, one per status the execution enters: RUNNING when
 * it starts, then the terminal SUCCEEDED, FAILED, STOPPED or TIMED_OUT.
 */
@ApplicationScoped
public class DurableExecutionEventPublisher {

    private static final Logger LOG = Logger.getLogger(DurableExecutionEventPublisher.class);
    static final String SOURCE = "aws.lambda";
    static final String DETAIL_TYPE = "Durable Execution Status Change";

    private final EventBridgeService eventBridgeService;
    private final ObjectMapper objectMapper;

    @Inject
    public DurableExecutionEventPublisher(EventBridgeService eventBridgeService, ObjectMapper objectMapper) {
        this.eventBridgeService = eventBridgeService;
        this.objectMapper = objectMapper;
    }

    /**
     * Builds the event from the execution as it is now. Call it under the execution's lock and run
     * the returned effect after the lock is released. Publishing never throws: a failure is logged
     * and the execution carries on.
     */
    public Runnable statusChange(DurableExecution execution) {
        String executionArn = execution.getExecutionArn();
        String region = execution.getRegion();
        String accountId = execution.getAccountId();
        String detail = buildDetail(execution).toString();
        return () -> {
            try {
                Map<String, Object> entry = EventBridgeService.serviceEventEntry(SOURCE, DETAIL_TYPE, detail,
                        executionArn);
                EventBridgeService.PutEventsResult result = eventBridgeService.putEvents(List.of(entry), region,
                        accountId);
                if (result != null && result.failedCount() > 0) {
                    LOG.warnv("EventBridge rejected Durable Execution Status Change for {0}: {1}",
                            executionArn, result.entries());
                }
            } catch (Exception e) {
                LOG.warnv("Failed to emit Durable Execution Status Change for {0}: {1}", executionArn,
                        e.getMessage());
            }
        };
    }

    private ObjectNode buildDetail(DurableExecution execution) {
        ObjectNode detail = objectMapper.createObjectNode();
        detail.put("durableExecutionArn", execution.getExecutionArn());
        detail.put("durableExecutionName", execution.getName());
        detail.put("functionArn", execution.getFunctionArn());
        detail.put("status", execution.getStatus().name());
        detail.put("startTimestamp", Instant.ofEpochMilli(execution.getStartTimestamp()).toString());
        if (execution.getEndTimestamp() != null) {
            detail.put("endTimestamp", Instant.ofEpochMilli(execution.getEndTimestamp()).toString());
        }
        return detail;
    }
}
