package com.floci.test;

import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.CreateEventSourceMappingRequest;
import software.amazon.awssdk.services.lambda.model.CreateFunctionRequest;
import software.amazon.awssdk.services.lambda.model.DeleteEventSourceMappingRequest;
import software.amazon.awssdk.services.lambda.model.DeleteFunctionRequest;
import software.amazon.awssdk.services.lambda.model.FunctionCode;
import software.amazon.awssdk.services.lambda.model.GetEventSourceMappingRequest;
import software.amazon.awssdk.services.lambda.model.ListEventSourceMappingsRequest;
import software.amazon.awssdk.services.lambda.model.Runtime;
import software.amazon.awssdk.services.lambda.model.UpdateEventSourceMappingRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Lambda - EventSourceMapping identity")
class LambdaEsmIdentityTest {

    private static final String FUNCTION_NAME = "sdk-test-esm-identity-fn";
    private static final String QUEUE_NAME = "sdk-test-esm-identity-queue";
    private static final String ROLE = "arn:aws:iam::000000000000:role/lambda-role";
    private static final String FUNCTION_ARN =
            "arn:aws:lambda:us-east-1:000000000000:function:" + FUNCTION_NAME;
    private static final String ESM_ARN_PREFIX =
            "arn:aws:lambda:us-east-1:000000000000:event-source-mapping:";

    private static LambdaClient lambda;
    private static SqsClient sqs;
    private static String queueUrl;
    private static String queueArn;
    private static String esmUuid;

    @BeforeAll
    static void setup() {
        lambda = TestFixtures.lambdaClient();
        sqs = TestFixtures.sqsClient();
        lambda.createFunction(CreateFunctionRequest.builder()
                .functionName(FUNCTION_NAME)
                .runtime(Runtime.NODEJS20_X)
                .role(ROLE)
                .handler("index.handler")
                .code(FunctionCode.builder()
                        .zipFile(SdkBytes.fromByteArray(LambdaUtils.minimalZip()))
                        .build())
                .build());
        queueUrl = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(QUEUE_NAME)
                .build())
                .queueUrl();
        queueArn = sqs.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build())
                .attributes()
                .get(QueueAttributeName.QUEUE_ARN);
    }

    @AfterAll
    static void cleanup() {
        if (lambda != null) {
            if (esmUuid != null) {
                try {
                    lambda.deleteEventSourceMapping(DeleteEventSourceMappingRequest.builder()
                            .uuid(esmUuid)
                            .build());
                } catch (Exception cleanupError) {
                    System.err.println("Could not delete ESM " + esmUuid + ": " + cleanupError.getMessage());
                }
            }
            try {
                lambda.deleteFunction(DeleteFunctionRequest.builder()
                        .functionName(FUNCTION_NAME)
                        .build());
            } catch (Exception cleanupError) {
                System.err.println("Could not delete function " + FUNCTION_NAME + ": "
                        + cleanupError.getMessage());
            }
            lambda.close();
        }
        if (sqs != null) {
            try {
                sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            } catch (Exception cleanupError) {
                System.err.println("Could not delete queue " + QUEUE_NAME + ": "
                        + cleanupError.getMessage());
            }
            sqs.close();
        }
    }

    @Test
    void createGetListAndUpdateKeepMappingAndFunctionArnsDistinct() {
        var created = lambda.createEventSourceMapping(
                CreateEventSourceMappingRequest.builder()
                        .functionName(FUNCTION_NAME)
                        .eventSourceArn(queueArn)
                        .batchSize(4)
                        .tags(Map.of(
                                "alchemy::id", "sdk-esm-identity",
                                "created-with", "request"))
                        .build());
        esmUuid = created.uuid();
        assertIdentity(created.uuid(), created.functionArn(), created.eventSourceMappingArn());

        var fetched = lambda.getEventSourceMapping(
                GetEventSourceMappingRequest.builder().uuid(esmUuid).build());
        assertIdentity(fetched.uuid(), fetched.functionArn(), fetched.eventSourceMappingArn());

        var listed = lambda.listEventSourceMappings(
                        ListEventSourceMappingsRequest.builder()
                                .functionName(FUNCTION_ARN)
                                .eventSourceArn(queueArn)
                                .build())
                .eventSourceMappings()
                .stream()
                .filter(mapping -> esmUuid.equals(mapping.uuid()))
                .findFirst()
                .orElseThrow();
        assertIdentity(listed.uuid(), listed.functionArn(), listed.eventSourceMappingArn());

        assertThat(lambda.listTags(request -> request.resource(created.eventSourceMappingArn())).tags())
                .containsEntry("alchemy::id", "sdk-esm-identity")
                .containsEntry("created-with", "request");

        lambda.tagResource(request -> request
                .resource(created.eventSourceMappingArn())
                .tags(Map.of("owner", "alchemy", "created-with", "tag-resource")));
        assertThat(lambda.listTags(request -> request.resource(created.eventSourceMappingArn())).tags())
                .containsEntry("alchemy::id", "sdk-esm-identity")
                .containsEntry("owner", "alchemy")
                .containsEntry("created-with", "tag-resource");

        var updated = lambda.updateEventSourceMapping(
                UpdateEventSourceMappingRequest.builder()
                        .uuid(esmUuid)
                        .functionName(FUNCTION_ARN)
                        .batchSize(8)
                        .build());
        assertIdentity(updated.uuid(), updated.functionArn(), updated.eventSourceMappingArn());
        assertThat(updated.batchSize()).isEqualTo(8);
        assertThat(lambda.listTags(request -> request.resource(updated.eventSourceMappingArn())).tags())
                .containsEntry("owner", "alchemy");

        lambda.untagResource(request -> request
                .resource(updated.eventSourceMappingArn())
                .tagKeys("created-with"));
        assertThat(lambda.listTags(request -> request.resource(updated.eventSourceMappingArn())).tags())
                .containsEntry("alchemy::id", "sdk-esm-identity")
                .containsEntry("owner", "alchemy")
                .doesNotContainKey("created-with");
    }

    private static void assertIdentity(String uuid, String functionArn, String eventSourceMappingArn) {
        assertThat(uuid).isEqualTo(esmUuid);
        assertThat(functionArn)
                .isEqualTo(FUNCTION_ARN)
                .contains(":function:")
                .doesNotContain(":event-source-mapping:");
        assertThat(eventSourceMappingArn)
                .isEqualTo(ESM_ARN_PREFIX + esmUuid)
                .contains(":event-source-mapping:")
                .doesNotContain(":function:");
    }
}
